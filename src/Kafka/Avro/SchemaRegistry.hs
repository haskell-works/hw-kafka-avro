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
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except (runExceptT, withExceptT, ExceptT(..))
import           Data.Aeson
import           Data.Avro.Schema (Schema)
import           Data.Cache as C
import           Data.Hashable
import           Data.Proxy
import qualified Data.Text.Encoding as Text
import           GHC.Exception
import           GHC.Generics (Generic)
import           Network.HTTP.Client (Manager, newManager, defaultManagerSettings)
import           Servant.API
import           Servant.Client

newtype SchemaId = SchemaId Int deriving (Eq, Ord, Show, Hashable)

newtype SchemaResponse = SchemaResponse Schema deriving (Generic, Show)

data SchemaRegistry = SchemaRegistry (Cache SchemaId Schema) Manager BaseUrl

data SchemaRegistryError = SchemaRegistryConnectError SomeException
                         | SchemaDecodeError SchemaId String
                         | SchemaRegistryResponseError SchemaId
                         | SchemaRegistryError SchemaId
                         deriving (Show)

schemaRegistry :: MonadIO m => String -> m SchemaRegistry
schemaRegistry url = liftIO $
  SchemaRegistry
  <$> newCache Nothing
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

type API = "schemas" :> "ids" :> Capture "id" Int :> Get '[JSON] SchemaResponse

api :: Proxy API
api = Proxy

apiLoadSchema :: Int -> Manager -> BaseUrl -> ClientM SchemaResponse
apiLoadSchema = client api

loadSchemaFromSR :: MonadIO m => Manager -> BaseUrl -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchemaFromSR m u sid@(SchemaId i) =
  liftExceptT (withExceptT convertError $ unwrapResponse <$> apiLoadSchema i m u)
  where
    unwrapResponse (SchemaResponse s) = s
    convertError msg = case msg of
      ConnectionError ex   -> SchemaRegistryConnectError ex
      FailureResponse{}    -> SchemaRegistryResponseError sid
      DecodeFailure de _ _ -> SchemaDecodeError sid de
      _                    -> SchemaRegistryError sid

instance FromJSON SchemaResponse where
  parseJSON (Object v) =
    withObject "expected schema" (\obj -> do
      sch <- obj .: "schema"
      maybe mempty (return . SchemaResponse) (decodeStrict $ Text.encodeUtf8 sch)
    ) (Object v)

  parseJSON _ = mempty

liftExceptT :: MonadIO m => ExceptT l IO r -> m (Either l r)
liftExceptT = liftIO . runExceptT
