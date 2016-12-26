module Kafka.Avro.Decode
(
  decodeWithSchema
) where

import           Control.Monad.IO.Class
import           Data.Avro as A
import           Data.Bits (shiftL)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL hiding (zipWith)
import           Kafka.Avro.SchemaRegistry

decodeWithSchema :: (MonadIO m, FromAvro a) => SchemaRegistry -> ByteString -> m (Either String a)
decodeWithSchema sr bs = case extractSchemaId bs of
  Nothing       -> return $ Left "Unable to determine schema ID"
  Just (n, bs') -> do
    s <- loadSchema sr n
    return $ case s of
      Left err -> Left ("Unable to load schema: " ++ show err)
      Right sc -> resultToEither $ A.decode sc bs'

extractSchemaId :: ByteString -> Maybe (SchemaId, ByteString)
extractSchemaId bs = do
  (w1, b1) <- BL.uncons bs
  (w2, b2) <- BL.uncons b1
  (w3, b3) <- BL.uncons b2
  (w4, b4) <- BL.uncons b3
  let int  =  sum $ fromIntegral <$> zipWith shiftL [w4, w3, w2, w1] [0, 8, 16, 24]
  return (SchemaId int, b4)

resultToEither :: A.Result a -> Either String a
resultToEither (Success a) = Right a
resultToEither (Error s) = Left s
