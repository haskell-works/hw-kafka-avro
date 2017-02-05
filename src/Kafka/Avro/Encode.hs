{-# LANGUAGE OverloadedStrings #-}
module Kafka.Avro.Encode
( encodeKey, encodeValue
, encodeWithSchema
) where

import           Control.Monad.IO.Class (MonadIO)
import           Data.Monoid
import           Data.Avro as A (ToAvro, schemaOf, encode)
import           Data.Avro.Schema (Schema)
import qualified Data.Binary as B
import           Data.Bits (shiftL)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL hiding (zipWith)
import           Kafka.Avro.SchemaRegistry

data EncodeError = EncodeRegistryError SchemaRegistryError

encodeKey :: (MonadIO m, ToAvro a)
          => SchemaRegistry
          -> Subject
          -> a
          -> m (Either EncodeError ByteString)
encodeKey sr (Subject subj) a =
  let keySubj = Subject (subj <> "-key")
   in encodeWithSchema sr keySubj a

encodeValue :: (MonadIO m, ToAvro a)
            => SchemaRegistry
            -> Subject
            -> a
            -> m (Either EncodeError ByteString)
encodeValue sr (Subject subj) a =
  let valSubj = Subject (subj <> "-value")
   in encodeWithSchema sr valSubj a

encodeWithSchema :: (MonadIO m, ToAvro a)
                 => SchemaRegistry
                 -> Subject
                 -> a
                 -> m (Either EncodeError ByteString)
encodeWithSchema sr subj a = do
  mbSid <- sendSchema sr subj (schemaOf a)
  case mbSid of
    Left err  -> return . Left . EncodeRegistryError $ err
    Right sid -> return . Right $ appendSchemaId sid (encode a)


appendSchemaId :: SchemaId -> ByteString -> ByteString
appendSchemaId (SchemaId sid) bs =
  -- add a "magic byte" followed by schema id
  BL.cons (toEnum 0) (B.encode sid) <> bs
