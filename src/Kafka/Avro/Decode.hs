{-# LANGUAGE ScopedTypeVariables #-}
module Kafka.Avro.Decode
(
  DecodeError(..)
, decodeWithSchema, extractSchemaId
) where

import           Control.Arrow             (left)
import           Control.Monad.IO.Class    (MonadIO)
import           Data.Avro                 as A (FromAvro, HasAvroSchema (..), Result (..), fromAvro)
import qualified Data.Avro                 as A (decodeWithSchema)
import qualified Data.Avro.Decode          as A (decodeAvro)
import qualified Data.Avro.Deconflict      as A (deconflict)
import           Data.Avro.Schema          (Schema)
import           Data.Bits                 (shiftL)
import           Data.ByteString.Lazy      (ByteString)
import qualified Data.ByteString.Lazy      as BL hiding (zipWith)
import           Data.Int
import           Data.Tagged               (Tagged, untag)
import           Kafka.Avro.SchemaRegistry

data DecodeError = DecodeRegistryError SchemaRegistryError
                 | BadPayloadNoSchemaId
                 | DecodeError Schema String
                 deriving (Show, Eq)

-- | Decodes a provided Avro-encoded value.
-- The serialised value is expected to be in a "confluent-compatible" format
-- where the "real" value bytestring is prepended with extra 5 bytes:
-- a "magic" byte and 4 bytes representing the schema ID.
decodeWithSchema :: (MonadIO m, FromAvro a)
                 => SchemaRegistry
                 -> ByteString
                 -> m (Either DecodeError a)
decodeWithSchema sr bs =
  case schemaData of
    Left err -> return $ Left err
    Right (sid, payload) -> do
      res <- left DecodeRegistryError <$> loadSchema sr sid
      return $ res >>= flip decodeWithDeconflict payload
  where
    schemaData = maybe (Left BadPayloadNoSchemaId) Right (extractSchemaId bs)

decodeWithDeconflict :: forall a. (FromAvro a) => Schema -> ByteString -> Either DecodeError a
decodeWithDeconflict writerSchema bs =
  let readerSchema = untag (schema :: Tagged a Schema)
  in left (DecodeError readerSchema) $ do
    raw <- A.decodeAvro writerSchema bs
    val <- A.deconflict writerSchema readerSchema raw
    resultToEither readerSchema (fromAvro val)

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

resultToEither :: Schema -> A.Result a -> Either String a
resultToEither sc res = case res of
  Success a -> Right a
  Error msg -> Left msg


