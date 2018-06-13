{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Kafka.Avro.SchemaRegistry
( schemaRegistry, loadSchema, sendSchema
, SchemaId(..), Subject(..)
, SchemaRegistry, SchemaRegistryError(..)
, RegisteredSchema(..)
) where

import           Control.Arrow           (first)
import           Control.Lens            (view, (^.))
import           Control.Monad           (void)
import           Control.Monad.IO.Class
import           Data.Aeson
import           Data.Avro.Schema        (Schema, Type (..), typeName)
import           Data.Bifunctor          (bimap)
import           Data.Cache              as C
import           Data.Hashable
import           Data.Int
import           Data.Text               (Text, append, cons, unpack)
import qualified Data.Text.Encoding      as Text
import qualified Data.Text.Lazy.Encoding as LText
import           GHC.Exception
import           GHC.Generics            (Generic)
import           Network.HTTP.Client     (HttpException (..), HttpExceptionContent (..), Manager, defaultManagerSettings, newManager)
import           Network.HTTP.Types
import qualified Network.Wreq            as Wreq

newtype SchemaId = SchemaId Int32 deriving (Eq, Ord, Show, Hashable)
newtype SchemaName = SchemaName Text deriving (Eq, Ord, Show, Hashable)

newtype Subject = Subject Text deriving (Eq, Show, Generic, Hashable)

newtype RegisteredSchema = RegisteredSchema { unRegisteredSchema :: Schema} deriving (Generic, Show)

data SchemaRegistry = SchemaRegistry
  { srCache        :: Cache SchemaId Schema
  , srReverseCache :: Cache (Subject, SchemaName) SchemaId
  , srBaseUrl      :: String
  }

data SchemaRegistryError = SchemaRegistryConnectError String
                         | SchemaRegistryLoadError SchemaId
                         | SchemaRegistrySchemaNotFound SchemaId
                         | SchemaRegistrySendError String
                         deriving (Show, Eq)

schemaRegistry :: MonadIO m => String -> m SchemaRegistry
schemaRegistry url = liftIO $
  SchemaRegistry
  <$> newCache Nothing
  <*> newCache Nothing
  <*> pure url

loadSchema :: MonadIO m => SchemaRegistry -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchema sr sid = do
  sc <- cachedSchema sr sid
  case sc of
    Just s  -> return (Right s)
    Nothing -> liftIO $ do
      res <- getSchemaById' (srBaseUrl sr) sid
      pure (unRegisteredSchema <$> res)

sendSchema :: MonadIO m => SchemaRegistry -> Subject -> Schema -> m (Either SchemaRegistryError SchemaId)
sendSchema sr subj sc = do
  sid <- cachedId sr subj schemaName
  case sid of
    Just sid' -> return (Right sid')
    Nothing   -> do
      res <- liftIO $ putSchema' (srBaseUrl sr) subj (RegisteredSchema sc)
      void $ traverse (cacheId sr subj schemaName) res
      void $ traverse (\sid' -> cacheSchema sr sid' sc) res
      pure res
  where
    schemaName = fullTypeName sc

------------------ PRIVATE: HELPERS --------------------------------------------

getSchemaById' :: String -> SchemaId -> IO (Either SchemaRegistryError RegisteredSchema)
getSchemaById' baseUrl sid@(SchemaId i) = do
  let schemaUrl = baseUrl ++ "/schemas/ids/" ++ show i
  resp <- Wreq.get schemaUrl
  pure $ bimap (const (SchemaRegistryLoadError sid)) (view Wreq.responseBody) (Wreq.asJSON resp)
  where
    wrapError :: SomeException -> SchemaRegistryError
    wrapError someErr = case fromException someErr of
      Nothing      -> SchemaRegistryLoadError sid
      Just httpErr -> fromHttpError httpErr $ \case
          StatusCodeException r _ | r ^. Wreq.responseStatus . Wreq.statusCode == 404 -> SchemaRegistrySchemaNotFound sid
          _                       -> SchemaRegistryLoadError sid

fromHttpError :: HttpException -> (HttpExceptionContent -> SchemaRegistryError) -> SchemaRegistryError
fromHttpError err f = case err of
  InvalidUrlException fld err'                      -> SchemaRegistryConnectError (fld ++ ": " ++ err')
  HttpExceptionRequest _ (ConnectionFailure err)    -> SchemaRegistryConnectError (displayException err)
  HttpExceptionRequest _ ConnectionTimeout          -> SchemaRegistryConnectError (displayException err)
  HttpExceptionRequest _ ProxyConnectException{}    -> SchemaRegistryConnectError (displayException err)
  HttpExceptionRequest _ ConnectionClosed           -> SchemaRegistryConnectError (displayException err)
  HttpExceptionRequest _ (InvalidProxySettings _)   -> SchemaRegistryConnectError (displayException err)
  HttpExceptionRequest _ (InvalidDestinationHost _) -> SchemaRegistryConnectError (displayException err)
  HttpExceptionRequest _ TlsNotSupported            -> SchemaRegistryConnectError (displayException err)
  HttpExceptionRequest _ err'                       -> f err'

putSchema' :: String -> Subject -> RegisteredSchema -> IO (Either SchemaRegistryError SchemaId)
putSchema' baseUrl (Subject sbj) schema = do
  let schemaUrl = baseUrl ++ "/subject/" ++ unpack sbj ++ "/versions"
  resp <- Wreq.post schemaUrl (toJSON schema)
  pure $ bimap wrapError (view Wreq.responseBody) (Wreq.asJSON resp)
  where
    wrapError :: SomeException -> SchemaRegistryError
    wrapError someErr = case fromException someErr of
      Nothing      -> SchemaRegistrySendError (displayException someErr)
      Just httpErr -> fromHttpError httpErr (\_ -> SchemaRegistrySendError (displayException someErr))

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

