{-# LANGUAGE OverloadedStrings #-}
module Message
( TestMessage(..)
) where
--

import           Data.Avro
import           Data.Int
import           Data.Text
import           Data.Avro.Schema
import qualified Data.Avro.Types as AT

data TestMessage = TestMessage Int64 Text Bool Int64 deriving (Show, Eq, Ord)

testMessageSchema =
  let fld nm = Field nm [] Nothing Nothing
   in Record "TestMessage" (Just "hw.kafka.avro.test") [] Nothing Nothing
         [ fld "id" Long Nothing
         , fld "name" String Nothing
         , fld "is_active" Boolean Nothing
         , fld "timestamp" Long Nothing
         ]

instance FromAvro TestMessage where
  fromAvro (AT.Record _ r) =
    TestMessage <$> r .: "id"
                <*> r .: "name"
                <*> r .: "is_active"
                <*> r .: "timestamp"
  fromAvro v = badValue v "TestMessage"

instance ToAvro TestMessage where
  toAvro (TestMessage i s d t) =
    record testMessageSchema
      [ "id"        .= i
      , "name"      .= s
      , "is_active" .= d
      , "timestamp" .= t
      ]
  schema = pure testMessageSchema
