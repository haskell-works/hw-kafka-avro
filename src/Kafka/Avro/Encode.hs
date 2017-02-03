module Kafka.Avro.Encode
( encodeWithSchema
) where

import           Control.Monad.IO.Class (MonadIO)
import           Data.Avro as A (ToAvro, schemaOf, encode)
import           Data.Avro.Schema (Schema)
import           Data.Bits (shiftL)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL hiding (zipWith)
import           Kafka.Avro.SchemaRegistry

data EncodeError = EncodeRegistryError SchemaRegistryError

encodeWithSchema :: (MonadIO m, ToAvro a)
                 => SchemaRegistry
                 -> a
                 -> m (Either EncodeError ByteString)
encodeWithSchema sr a =
  let aSchema = schemaOf a
      payload = encode a
   in undefined
