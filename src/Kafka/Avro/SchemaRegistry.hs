{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Kafka.Avro.SchemaRegistry
( schemaRegistry, loadSchema
, SchemaId(..)
, SchemaRegistry, SchemaRegistryError(..)
, RegisteredSchema(..)
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except (runExceptT, withExceptT, ExceptT(..))
import           Data.Aeson
import           Data.Avro.Schema (Schema)
import           Data.Cache as C
import           Data.Hashable
import           Data.Proxy
import           Data.Text (Text)
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Lazy.Encoding as LText
import           GHC.Exception
import           GHC.Generics (Generic)
import           Network.HTTP.Client (Manager, newManager, defaultManagerSettings)
import           Servant.API
import           Servant.Client

newtype SchemaId = SchemaId Int deriving (Eq, Ord, Show, Hashable)

newtype Subject = Subject Text deriving (Eq, Show, Generic)

newtype RegisteredSchema = RegisteredSchema Schema deriving (Generic, Show)

data SchemaRegistry = SchemaRegistry
  { srCache         :: Cache SchemaId Schema
  , srReverseCache  :: Cache (Subject, Schema) SchemaId
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
loadSchema (SchemaRegistry c m u) sid = do
  sc <- liftIO $ C.lookup c sid
  case sc of
    Just s  -> return (Right s)
    Nothing -> do
       res <- loadSchemaFromSR m u sid
       _   <- traverse (liftIO . C.insert c sid) res
       return res

------------------ PRIVATE: HELPERS --------------------------------------------

type API = "schemas" :> "ids" :> Capture "id" Int :> Get '[JSON] RegisteredSchema
      :<|> "schemas" :> Capture "subject" Subject :> ReqBody '[JSON] RegisteredSchema :> Post '[JSON] SchemaId
api :: Proxy API
api = Proxy

getSchemaById :: Int -> Manager -> BaseUrl -> ClientM RegisteredSchema
putSchema :: Subject -> RegisteredSchema -> Manager -> BaseUrl -> ClientM SchemaId
getSchemaById :<|> putSchema = client api

sendSchemaToSR :: MonadIO m => Manager -> BaseUrl -> Subject -> Schema -> m (Either SchemaRegistryError SchemaId)
sendSchemaToSR m u subj s =
  liftExceptT . withExceptT toSRError $ putSchema subj (RegisteredSchema s) m u
  where
    toSRError msg = case msg of
      ConnectionError ex   -> SchemaRegistryConnectError ex
      _                    -> undefined

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

cachedSchema :: MonadIO m => SchemaRegistry -> SchemaId -> m (Maybe Schema)
cachedSchema sr k = liftIO $ C.lookup (srCache sr) k

cachedId :: MonadIO m => SchemaRegistry -> (Subject, Schema) -> m (Maybe SchemaId)
cachedId sr k = liftIO $ C.lookup (srReverseCache sr) k

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
