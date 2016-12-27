{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Monad.Trans.Except
import           Data.Avro
import qualified Data.Avro.Types as AT
import           Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Text
import           Kafka.Avro.Decode
import           Kafka.Avro.SchemaRegistry


data DedupInfo = DedupInfo Int64 Text Bool Int64 deriving (Show, Eq, Ord)

instance FromAvro DedupInfo where
  fromAvro (AT.Record _ r) =
    DedupInfo <$> r .: "id"
              <*> r .: "submitter_ip"
              <*> r .: "is_duplicate"
              <*> r .: "timestamp"
  fromAvro v = badValue v "DedupInfo"

main :: IO ()
main = do
  sr  <- schemaRegistry "http://localhost:8081"
  res <- runExceptT $ decodeWithSchema sr bsWithSchemaId
  showDedupInfo res

bsWithSchemaId :: BL.ByteString
bsWithSchemaId = BL.pack [0,0,0,2,65,66,67,68,69]

showDedupInfo :: Either DecodeError DedupInfo -> IO ()
showDedupInfo = print . show
