module Kafka.Avro.Decode
(
  DecodeError(..)
, decodeWithSchema
) where

import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Trans.Except (withExceptT, ExceptT(..))
import           Control.Error.Util ((??), hoistEither)
import           Data.Avro as A (FromAvro, Result(..), decode)
import           Data.Avro.Schema (Schema)
import           Data.Bits (shiftL)
import           Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BL hiding (zipWith)
import           Kafka.Avro.SchemaRegistry

data DecodeError = RegistryError SchemaRegistryError
                 | BadPayloadNoSchemaId
                 | DecodeError Schema String
                 deriving (Show)

decodeWithSchema :: (MonadIO m, FromAvro a) => SchemaRegistry -> ByteString -> ExceptT DecodeError m a
decodeWithSchema sr bs = do
  (n, bs') <- extractSchemaId bs ?? BadPayloadNoSchemaId
  s        <- withExceptT RegistryError (loadSchema sr n)
  hoistEither $ resultToEither s (A.decode s bs')

extractSchemaId :: ByteString -> Maybe (SchemaId, ByteString)
extractSchemaId bs = do
  (_ , b0) <- BL.uncons bs
  (w1, b1) <- BL.uncons b0
  (w2, b2) <- BL.uncons b1
  (w3, b3) <- BL.uncons b2
  (w4, b4) <- BL.uncons b3
  let int  =  sum $ fromIntegral <$> zipWith shiftL [w4, w3, w2, w1] [0, 8, 16, 24]
  return (SchemaId int, b4)

resultToEither :: Schema -> A.Result a -> Either DecodeError a
resultToEither sc res = case res of
  Success a -> Right a
  Error msg -> Left $ DecodeError sc msg
