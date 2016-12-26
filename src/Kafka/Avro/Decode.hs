module Kafka.Avro.Decode
(
  DecodeError(..)
, decodeWithSchema
) where

import           Control.Monad.IO.Class
import           Data.Avro as A
import           Data.Avro.Schema (Schema)
import           Data.Bits (shiftL)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL hiding (zipWith)
import           Kafka.Avro.SchemaRegistry

data DecodeError = RegistryError SchemaRegistryError
                 | BadPayloadNoSchemaId
                 | DecodeError Schema String
                 deriving (Show)

decodeWithSchema :: (MonadIO m, FromAvro a) => SchemaRegistry -> ByteString -> m (Either DecodeError a)
decodeWithSchema sr bs = case extractSchemaId bs of
  Nothing       -> return $ Left BadPayloadNoSchemaId
  Just (n, bs') -> do
    s <- loadSchema sr n
    return $ case s of
      Left err -> Left $ RegistryError err
      Right sc -> resultToEither sc (A.decode sc bs')

extractSchemaId :: ByteString -> Maybe (SchemaId, ByteString)
extractSchemaId bs = do
  (w1, b1) <- BL.uncons bs
  (w2, b2) <- BL.uncons b1
  (w3, b3) <- BL.uncons b2
  (w4, b4) <- BL.uncons b3
  let int  =  sum $ fromIntegral <$> zipWith shiftL [w4, w3, w2, w1] [0, 8, 16, 24]
  return (SchemaId int, b4)

resultToEither :: Schema -> A.Result a -> Either DecodeError a
resultToEither sc res = case res of
  Success a -> Right a
  Error err -> Left $ DecodeError sc err
