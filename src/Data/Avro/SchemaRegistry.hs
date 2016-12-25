{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE TypeOperators              #-}
{-# LANGUAGE ScopedTypeVariables        #-}
module Data.Avro.SchemaRegistry
( schemaRegistry, loadSchema
, SchemaId(..)
, SchemaRegistry, SchemaRegistryError(..)
) where

import           Control.Arrow (left, right)
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Except (runExceptT)
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

data SchemaRegistryError = ConnectError SomeException
                         | DecodeError SchemaId String
                         | ResponseError SchemaId
                         | SchemaRegistryError SchemaId
                         deriving (Show)

schemaRegistry :: MonadIO m => String -> m SchemaRegistry
schemaRegistry url = liftIO $
  SchemaRegistry
  <$> newCache Nothing
  <*> newManager defaultManagerSettings
  <*> parseBaseUrl url

loadSchema :: MonadIO m => SchemaRegistry -> SchemaId -> m (Either SchemaRegistryError Schema)
loadSchema (SchemaRegistry c m u) sid = liftIO $
   C.lookup c sid >>= \sc -> case sc of
    Just s  -> return $ Right s
    Nothing -> do
       res <- loadSchema' m u sid
       _   <- sequence $ C.insert c sid <$> res
       return res

------------------ PRIVATE: HELPERS --------------------------------------------

type API = "schemas" :> "ids" :> Capture "id" Int :> Get '[JSON] SchemaResponse

api :: Proxy API
api = Proxy

apiLoadSchema :: Int -> Manager -> BaseUrl -> ClientM SchemaResponse
apiLoadSchema = client api

convertError :: SchemaId -> ServantError -> SchemaRegistryError
convertError sid err = case err of
  ConnectionError ex   -> ConnectError ex
  FailureResponse{}    -> ResponseError sid
  DecodeFailure de _ _ -> DecodeError sid de
  _                    -> SchemaRegistryError sid

unwrapResponse :: SchemaResponse -> Schema
unwrapResponse (SchemaResponse s) = s

loadSchema' :: Manager -> BaseUrl -> SchemaId -> IO (Either SchemaRegistryError Schema)
loadSchema' m u sid@(SchemaId i) =
  right unwrapResponse . left (convertError sid) <$> runExceptT (apiLoadSchema i m u)

instance FromJSON SchemaResponse where
  parseJSON (Object v) =
    withObject "expected schema" (\obj -> do
      sch <- obj .: "schema"
      maybe mempty (return . SchemaResponse) (decodeStrict $ Text.encodeUtf8 sch)
    ) (Object v)

  parseJSON _ = mempty
