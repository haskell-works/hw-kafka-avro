{-# LANGUAGE OverloadedStrings, ScopedTypeVariables #-}
module Main where

import           Control.Monad.Trans.Except
import           Data.Monoid
import qualified Data.Aeson as J
import           Data.Avro as A
import           Data.Avro.Schema as S
import qualified Data.Avro.Types as AT

import           Data.Int
import           Data.Text
import           Kafka.Avro
import           Message

exampleMessage = TestMessage 1 "Example" True 12345678

data AppError = EncError EncodeError | DecError DecodeError
  deriving (Show)

main :: IO ()
main = do
  sr   <- schemaRegistry "http://localhost:8081"
  res  <- runExceptT $ roundtrip sr
  print res

roundtrip :: SchemaRegistry -> ExceptT AppError IO TestMessage
roundtrip sr = do
  enc <- withExceptT EncError (encode exampleMessage)
  dec <- withExceptT DecError (decode enc)
  return dec
  where
    encode msg = ExceptT $ encodeWithSchema sr (Subject "example-subject") exampleMessage
    decode msg = ExceptT $ decodeWithSchema sr msg
