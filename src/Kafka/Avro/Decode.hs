{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Kafka.Avro.Decode
(
  DecodeError(..)
, decode
, decodeWithSchema
, extractSchemaId
) where

import           Control.Arrow              (left)
import           Control.Monad.IO.Class     (MonadIO)
import           Control.Monad.Trans.Except
import           Data.Avro                  (FromAvro, HasAvroSchema (..), Schema, decodeValueWithSchema, deconflict)
import           Data.Bits                  (shiftL)
import           Data.ByteString.Lazy       (ByteString)
import qualified Data.ByteString.Lazy       as BL hiding (zipWith)
import           Data.Int
import           Data.Tagged                (untag)
import           Kafka.Avro.SchemaRegistry

data DecodeError = DecodeRegistryError SchemaRegistryError
                 | BadPayloadNoSchemaId
                 | DecodeError Schema String
                 | IncompatibleSchema Schema String
                 deriving (Show, Eq)

-- | Decodes a provided Avro-encoded value.
-- The serialised value is expected to be in a "confluent-compatible" format
-- where the "real" value bytestring is prepended with extra 5 bytes:
-- a "magic" byte and 4 bytes representing the schema ID.
decode :: forall a m. (MonadIO m, HasAvroSchema a, FromAvro a)
  => SchemaRegistry
  -> ByteString
  -> m (Either DecodeError a)
decode sr = decodeWithSchema sr (untag @a schema)
{-# INLINE decode #-}

decodeWithSchema :: forall a m. (MonadIO m, FromAvro a)
  => SchemaRegistry
  -> Schema
  -> ByteString
  -> m (Either DecodeError a)
decodeWithSchema sr readerSchema bs =
  case schemaData of
    Left err -> return $ Left err
    Right (sid, payload) -> runExceptT $ do
      writerSchema  <- withError DecodeRegistryError (loadSchema sr sid)
      readSchema    <- withPureError (IncompatibleSchema writerSchema) $ deconflict writerSchema readerSchema
      withPureError (DecodeError writerSchema) (decodeValueWithSchema readSchema payload)
  where
    schemaData = maybe (Left BadPayloadNoSchemaId) Right (extractSchemaId bs)
    withError f = withExceptT f . ExceptT
    withPureError f = withError f . pure

extractSchemaId :: ByteString -> Maybe (SchemaId, ByteString)
extractSchemaId bs = do
  (_ , b0) <- BL.uncons bs
  (w1, b1) <- BL.uncons b0
  (w2, b2) <- BL.uncons b1
  (w3, b3) <- BL.uncons b2
  (w4, b4) <- BL.uncons b3
  let ints =  fromIntegral <$> [w4, w3, w2, w1] :: [Int32]
  let int  =  sum $ zipWith shiftL ints [0, 8, 16, 24]
  return (SchemaId int, b4)

