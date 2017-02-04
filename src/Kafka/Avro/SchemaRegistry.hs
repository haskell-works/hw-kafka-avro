{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Kafka.Avro.SchemaRegistry
( schemaRegistry, loadSchema, sendSchema
, SchemaId(..), Subject(..)
, SchemaRegistry, SchemaRegistryError(..)
, RegisteredSchema(..)
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except (runExceptT, withExceptT, ExceptT(..))
import           Data.Aeson
import           Data.Avro.Schema (Schema, Type(..), typeName)
import           Data.Cache as C
import           Data.Hashable
import           Data.Proxy
import           Data.Text (Text, append, cons)
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Lazy.Encoding as LText
import           GHC.Exception
import           GHC.Generics (Generic)
import           Network.HTTP.Client (Manager, newManager, defaultManagerSettings)
import           Servant.API
import           Servant.Client

newtype SchemaId = SchemaId Int deriving (Eq, Ord, Show, Hashable)
newtype SchemaName = SchemaName Text deriving (Eq, Ord, Show, Hashable)

newtype Subject = Subject Text deriving (Eq, Show, Generic, Hashable)

newtype RegisteredSchema = RegisteredSchema Schema deriving (Generic, Show)

data SchemaRegistry = SchemaRegistry
  { srCache         :: Cache SchemaId Schema
  , srReverseCache  :: Cache (Subject, SchemaName) SchemaId
  , srManager       :: Manager
  , srBaseUrl       :: BaseUrl
  }

data SchemaRegistryError = SchemaRegistryConnectError SomeException
                         | SchemaDecodeError SchemaId String
                         | SchemaRegistryLoadError SchemaId
                         | SchemaRegistryError SchemaId
                         deriving (Show)

schemaRegistry :: MonadIO m => String -> m SchemaRegistry
schemaRegistry url = liftIO $
  SchemaRegistry
  <$> newCache Nothing
  <*> newCache Nothing
  <*> newManager defaultManagerSettings
  <*> parseBaseUrl url

loadSchema :: MonadIO m => SchemaRegistry -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchema sr sid = do
  sc <- cachedSchema sr sid
  case sc of
    Just s  -> return (Right s)
    Nothing -> do
       res <- loadSchemaFromSR (srManager sr) (srBaseUrl sr) sid
       _   <- traverse (cacheSchema sr sid) res
       return res

sendSchema :: MonadIO m => SchemaRegistry -> Subject -> Schema -> m (Either SchemaRegistryError SchemaId)
sendSchema sr subj sc = do
  sid <- cachedId sr subj schemaName
  case sid of
    Just sid' -> return (Right sid')
    Nothing   -> do
      res <- sendSchemaToSR (srManager sr) (srBaseUrl sr) subj sc
      _   <- traverse (cacheId sr subj schemaName) res
      return res
  where
    schemaName = fullTypeName sc

------------------ PRIVATE: HELPERS --------------------------------------------

type API = "schemas" :> "ids" :> Capture "id" Int :> Get '[JSON] RegisteredSchema
      :<|> "subjects" :> Capture "subject" Subject :> "versions" :> ReqBody '[JSON] RegisteredSchema :> Post '[JSON] SchemaId
api :: Proxy API
api = Proxy

getSchemaById :: Int -> Manager -> BaseUrl -> ClientM RegisteredSchema
putSchema :: Subject -> RegisteredSchema -> Manager -> BaseUrl -> ClientM SchemaId
getSchemaById :<|> putSchema = client api

type P = ServantError

sendSchemaToSR :: MonadIO m => Manager -> BaseUrl -> Subject -> Schema -> m (Either SchemaRegistryError SchemaId)
sendSchemaToSR m u subj s =
  liftExceptT . withExceptT toSRError $ putSchema subj (RegisteredSchema s) m u
  where
    toSRError msg = case msg of
      ConnectionError ex   -> SchemaRegistryConnectError ex
      DecodeFailure de _ _ -> undefined

loadSchemaFromSR :: MonadIO m => Manager -> BaseUrl -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchemaFromSR m u sid@(SchemaId i) =
  liftExceptT (withExceptT toSRError $ unwrapResponse <$> getSchemaById i m u)
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
  parseJSON _ = mempty

instance ToHttpApiData Subject where
  toUrlPiece (Subject s) = toUrlPiece s

liftExceptT :: MonadIO m => ExceptT l IO r -> m (Either l r)
liftExceptT = liftIO . runExceptT
