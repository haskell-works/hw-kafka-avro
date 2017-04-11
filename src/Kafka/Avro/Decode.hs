module Kafka.Avro.Decode
(
  DecodeError(..)
, decodeWithSchema
) where

import           Control.Monad.IO.Class    (MonadIO)
import           Data.Avro                 as A (FromAvro, Result (..), decode)
import           Data.Avro.Schema          (Schema)
import           Data.Bits                 (shiftL)
import           Data.ByteString.Lazy      (ByteString)
import qualified Data.ByteString.Lazy      as BL hiding (zipWith)
import           Data.Int
import           Kafka.Avro.SchemaRegistry

data DecodeError = DecodeRegistryError SchemaRegistryError
                 | BadPayloadNoSchemaId
                 | DecodeError Schema String
                 | InvalidSchema
                 deriving (Show)

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
      res <- leftMap DecodeRegistryError <$> loadSchema sr sid
      return $ res >>= decode payload
  where
    schemaData = maybe (Left BadPayloadNoSchemaId) Right (extractSchemaId bs)
    decode p s = resultToEither s (A.decode s p)

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

leftMap :: (e -> e') -> Either e r -> Either e' r
leftMap _ (Right r) = Right r
leftMap f (Left e)  = Left (f e)

resultToEither :: Schema -> A.Result a -> Either DecodeError a
resultToEither sc res = case res of
  Success a -> Right a
  Error msg -> Left $ DecodeError sc msg
