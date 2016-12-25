module Main where

import Data.Avro.SchemaRegistry

main :: IO ()
main = do
  sr  <- schemaRegistry "http://localhost:8081"
  res <- loadSchema sr (SchemaId 2)
  print res
