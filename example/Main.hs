{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Monad.Trans.Except
import qualified Data.Aeson as J
import           Data.Avro as A
import           Data.Avro.Schema as S
import qualified Data.Avro.Types as AT
import           Data.ByteString.Lazy as BL
import           Data.Int
import           Data.Text
import           Kafka.Avro.Decode
import           Kafka.Avro.SchemaRegistry


data DedupInfo = DedupInfo Int64 Text Bool Int64 deriving (Show, Eq, Ord)

dedupSchema =
  let fld nm = Field nm [] Nothing Nothing
   in Record "DedupInfo" (Just "hw.kafka.avro.test") [] Nothing Nothing
         [ fld "id" Long Nothing
         , fld "submitter_ip" String Nothing
         , fld "is_duplicate" Boolean Nothing
         , fld "timestamp" Long Nothing
         ]

instance FromAvro DedupInfo where
  fromAvro (AT.Record _ r) =
    DedupInfo <$> r .: "id"
              <*> r .: "submitter_ip"
              <*> r .: "is_duplicate"
              <*> r .: "timestamp"
  fromAvro v = badValue v "DedupInfo"

instance ToAvro DedupInfo where
  toAvro (DedupInfo i s d t) =
    record dedupSchema [ "id"           .= i
                       , "submitter_ip" .= s
                       , "is_duplicate" .= d
                       , "timestamp"    .= t
                       ]
  schema = pure dedupSchema

main :: IO ()
main = do
  test
  sr   <- schemaRegistry "http://localhost:8081"
  sent <- sendSchema sr (Subject "test-subject") dedupSchema
  print sent
  -- res <- decodeWithSchema sr bsWithSchemaId
  -- showDedupInfo res

test =
  let dinfo = DedupInfo 1 "abc" True 1
      rs = RegisteredSchema (schemaOf dinfo)
   in print $ J.encode rs

bsWithSchemaId :: BL.ByteString
bsWithSchemaId = BL.pack [0,0,0,0,1,65,66,67,68,69]

showDedupInfo :: Either DecodeError DedupInfo -> IO ()
showDedupInfo = print . show
