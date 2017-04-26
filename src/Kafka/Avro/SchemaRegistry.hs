{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeOperators              #-}

module Kafka.Avro.SchemaRegistry
( schemaRegistry, loadSchema, sendSchema
, SchemaId(..), Subject(..)
, SchemaRegistry, SchemaRegistryError(..)
, RegisteredSchema(..)
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except (ExceptT (..), runExceptT, withExceptT)
import           Data.Aeson
import           Data.Avro.Schema           (Schema, Type (..), typeName)
import           Data.Cache                 as C
import           Data.Hashable
import           Data.Int
import           Data.Proxy
import           Data.Text                  (Text, append, cons)
import qualified Data.Text.Encoding         as Text
import qualified Data.Text.Lazy.Encoding    as LText
import           GHC.Exception
import           GHC.Generics               (Generic)
import           Network.HTTP.Client        (Manager, defaultManagerSettings, newManager)
import           Servant.API
import           Servant.Client

newtype SchemaId = SchemaId Int32 deriving (Eq, Ord, Show, Hashable)
newtype SchemaName = SchemaName Text deriving (Eq, Ord, Show, Hashable)

newtype Subject = Subject Text deriving (Eq, Show, Generic, Hashable)

newtype RegisteredSchema = RegisteredSchema Schema deriving (Generic, Show)

data SchemaRegistry = SchemaRegistry
  { srCache        :: Cache SchemaId Schema
  , srReverseCache :: Cache (Subject, SchemaName) SchemaId
#if MIN_VERSION_servant(0,9,1)
  , srClientEnv    :: ClientEnv
#else
  , srManager      :: Manager
  , srBaseUrl      :: BaseUrl
#endif
  }

data SchemaRegistryError = SchemaRegistryConnectError SomeException
                         | SchemaDecodeError SchemaId String
                         | SchemaRegistryLoadError SchemaId
                         | SchemaRegistryError SchemaId
                         | SchemaRegistrySendError String
                         deriving (Show)

schemaRegistry :: MonadIO m => String -> m SchemaRegistry
schemaRegistry url = liftIO $
  SchemaRegistry
  <$> newCache Nothing
  <*> newCache Nothing
#if MIN_VERSION_servant(0,9,1)
  <*> (ClientEnv <$> newManager defaultManagerSettings <*> parseBaseUrl url)
#else
  <*> newManager defaultManagerSettings
  <*> parseBaseUrl url
#endif

loadSchema :: MonadIO m => SchemaRegistry -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchema sr sid = do
  sc <- cachedSchema sr sid
  case sc of
    Just s  -> return (Right s)
    Nothing -> do
#if MIN_VERSION_servant(0,9,1)
       res <- loadSchemaFromSR (srClientEnv sr) sid
#else
       res <- loadSchemaFromSR (srManager sr) (srBaseUrl sr) sid
#endif
       _   <- traverse (cacheSchema sr sid) res
       return res

sendSchema :: MonadIO m => SchemaRegistry -> Subject -> Schema -> m (Either SchemaRegistryError SchemaId)
sendSchema sr subj sc = do
  sid <- cachedId sr subj schemaName
  case sid of
    Just sid' -> return (Right sid')
    Nothing   -> do
#if MIN_VERSION_servant(0,9,1)
      res <- sendSchemaToSR (srClientEnv sr) subj sc
#else
      res <- sendSchemaToSR (srManager sr) (srBaseUrl sr) subj sc
#endif
      _   <- traverse (cacheId sr subj schemaName) res
      _   <- traverse (\sid' -> cacheSchema sr sid' sc) res
      return res
  where
    schemaName = fullTypeName sc

------------------ PRIVATE: HELPERS --------------------------------------------

type API = "schemas" :> "ids" :> Capture "id" Int32 :> Get '[JSON] RegisteredSchema
      :<|> "subjects" :> Capture "subject" Subject :> "versions" :> ReqBody '[JSON] RegisteredSchema :> Post '[JSON] SchemaId
api :: Proxy API
api = Proxy

#if MIN_VERSION_servant(0,9,1)
--ExceptT ServantError IO SchemaId
getSchemaById :: Int32 -> ClientM RegisteredSchema
putSchema :: Subject -> RegisteredSchema -> ClientM SchemaId
#else
getSchemaById :: Int32 -> Manager -> BaseUrl -> ClientM RegisteredSchema
putSchema :: Subject -> RegisteredSchema -> Manager -> BaseUrl -> ClientM SchemaId
#endif
getSchemaById :<|> putSchema = client api

type P = ServantError

#if MIN_VERSION_servant(0,9,1)
sendSchemaToSR :: MonadIO m => ClientEnv -> Subject -> Schema -> m (Either SchemaRegistryError SchemaId)
sendSchemaToSR env subj s =
  runExceptT $ withExceptT toSRError $ runServant env $ putSchema subj (RegisteredSchema s)
#else
sendSchemaToSR :: MonadIO m => Manager -> BaseUrl -> Subject -> Schema -> m (Either SchemaRegistryError SchemaId)
sendSchemaToSR m u subj s =
  liftExceptT . withExceptT toSRError $ putSchema subj (RegisteredSchema s) m u
#endif
  where
    toSRError msg = case msg of
      ConnectionError ex   -> SchemaRegistryConnectError ex
      DecodeFailure de _ _ -> SchemaRegistrySendError de
      err                  -> SchemaRegistrySendError (show err)

#if MIN_VERSION_servant(0,9,1)
loadSchemaFromSR :: MonadIO m => ClientEnv -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchemaFromSR env sid@(SchemaId i) =
  runExceptT (withExceptT toSRError $ runServant env $ unwrapResponse <$> getSchemaById i)
#else
loadSchemaFromSR :: MonadIO m => Manager -> BaseUrl -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchemaFromSR m u sid@(SchemaId i) =
  liftExceptT (withExceptT toSRError $ unwrapResponse <$> getSchemaById i m u)
#endif
  where
    unwrapResponse (RegisteredSchema s) = s
    toSRError msg = case msg of
      ConnectionError ex   -> SchemaRegistryConnectError ex
      FailureResponse{}    -> SchemaRegistryLoadError sid
      DecodeFailure de _ _ -> SchemaDecodeError sid de
      _                    -> SchemaRegistryLoadError sid

---------------------------------------------------------------------
fullTypeName :: Schema -> SchemaName
fullTypeName r = SchemaName $ case r of
  Record{} -> maybe (typeName r)
                    (\ns -> ns `append` ('.' `cons` typeName r))
                    (namespace r)
  _        -> typeName r

cachedSchema :: MonadIO m => SchemaRegistry -> SchemaId -> m (Maybe Schema)
cachedSchema sr k = liftIO $ C.lookup (srCache sr) k
{-# INLINE cachedSchema #-}

cacheSchema :: MonadIO m => SchemaRegistry -> SchemaId -> Schema -> m ()
cacheSchema sr k v = liftIO $ C.insert (srCache sr) k v
{-# INLINE cacheSchema #-}

cachedId :: MonadIO m => SchemaRegistry -> Subject -> SchemaName -> m (Maybe SchemaId)
cachedId sr subj scn = liftIO $ C.lookup (srReverseCache sr) (subj, scn)
{-# INLINE cachedId #-}

cacheId :: MonadIO m => SchemaRegistry -> Subject -> SchemaName -> SchemaId -> m ()
cacheId sr subj scn sid = liftIO $ C.insert (srReverseCache sr) (subj, scn) sid
{-# INLINE cacheId #-}

instance FromJSON RegisteredSchema where
  parseJSON (Object v) =
    withObject "expected schema" (\obj -> do
      sch <- obj .: "schema"
      maybe mempty (return . RegisteredSchema) (decode $ LText.encodeUtf8 sch)
    ) (Object v)

  parseJSON _ = mempty

instance ToJSON RegisteredSchema where
  toJSON (RegisteredSchema v) = object ["schema" .= LText.decodeUtf8 (encode $ toJSON v)]

instance FromJSON SchemaId where
  parseJSON (Object v) = SchemaId <$> v .: "id"
  parseJSON _          = mempty

instance ToHttpApiData Subject where
  toUrlPiece (Subject s) = toUrlPiece s

#if MIN_VERSION_servant(0,9,1)
runServant :: MonadIO m => ClientEnv -> ClientM a -> ExceptT ServantError m a
runServant env cli = ExceptT $ liftIO (runClientM cli env)
#else
liftExceptT :: MonadIO m => ExceptT l IO r -> m (Either l r)
liftExceptT = liftIO . runExceptT
#endif
