{-# LANGUAGE OverloadedStrings #-}
module Main where

import Data.Avro
import qualified Data.Avro.Types as AT
import Data.Int
import Data.Text
import Kafka.Avro.SchemaRegistry
import Kafka.Avro.Decode
import Data.ByteString.Lazy as BL


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
  res <- decodeWithSchema sr bsWithSchemaId
  showDedupInfo res


bsWithSchemaId :: BL.ByteString
bsWithSchemaId = BL.pack [0,0,0,2,65,65,65,65]

showDedupInfo :: Either String DedupInfo -> IO ()
showDedupInfo = print . show
