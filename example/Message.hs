{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TemplateHaskell   #-}
module Message
( TestMessage(..)
, schema'TestMessage
) where

import Data.Avro
import Data.Avro.Deriving

deriveAvroFromByteString [r|
{
  "type": "record",
  "name": "TestMessage",
  "namespace": "hw.kafka.avro.test",
  "fields": [
    { "name": "id", "type": "long" },
    { "name": "name", "type": "string" },
    { "name": "is_active", "type": "boolean" },
    { "name": "timestamp", "type": "long" }
  ]
}
|]

