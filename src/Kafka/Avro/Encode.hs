{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use newtype instead of data" #-}
module Kafka.Avro.Encode
( EncodeError(..)
, encodeKey
, encodeValue
, encode

, encodeKeyWithSchema
, encodeValueWithSchema
, encodeWithSchema

, keySubject, valueSubject
) where

import           Control.Monad.IO.Class    (MonadIO)
import           Data.Avro                 (HasAvroSchema, Schema, ToAvro, schemaOf)
import qualified Data.Avro                 as A
import qualified Data.Binary               as B
import           Data.Bits                 (shiftL)
import           Data.ByteString.Lazy      (ByteString)
import qualified Data.ByteString.Lazy      as BL hiding (zipWith)
import           Data.Monoid
import           Kafka.Avro.SchemaRegistry

data EncodeError = EncodeRegistryError SchemaRegistryError
  deriving (Show, Eq)

keySubject :: Subject -> Subject
keySubject (Subject subj) = Subject (subj <> "-key")
{-# INLINE keySubject #-}

valueSubject :: Subject -> Subject
valueSubject (Subject subj) = Subject (subj <> "-value")
{-# INLINE valueSubject #-}

-- | Encodes a provided value as a message key.
--
-- Registers the schema in SchemaRegistry with "<subject>-key" subject.
encodeKey :: (MonadIO m, HasAvroSchema a, ToAvro a)
  => SchemaRegistry
  -> Subject
  -> a
  -> m (Either EncodeError ByteString)
encodeKey sr subj = encode sr (keySubject subj)
{-# INLINE encodeKey #-}

-- | Encodes a provided value as a message key.
--
-- Registers the schema in SchemaRegistry with "<subject>-key" subject.
encodeKeyWithSchema :: (MonadIO m, ToAvro a)
  => SchemaRegistry
  -> Subject
  -> Schema
  -> a
  -> m (Either EncodeError ByteString)
encodeKeyWithSchema sr subj = encodeWithSchema sr (keySubject subj)
{-# INLINE encodeKeyWithSchema #-}

-- | Encodes a provided value as a message value.
--
-- Registers the schema in SchemaRegistry with "<subject>-value" subject.
encodeValue :: (MonadIO m, HasAvroSchema a, ToAvro a)
  => SchemaRegistry
  -> Subject
  -> a
  -> m (Either EncodeError ByteString)
encodeValue sr subj = encode sr (valueSubject subj)
{-# INLINE encodeValue #-}

-- | Encodes a provided value as a message value.
--
-- Registers the schema in SchemaRegistry with "<subject>-value" subject.
encodeValueWithSchema :: (MonadIO m, ToAvro a)
  => SchemaRegistry
  -> Subject
  -> Schema
  -> a
  -> m (Either EncodeError ByteString)
encodeValueWithSchema sr subj = encodeWithSchema sr (valueSubject subj)
{-# INLINE encodeValueWithSchema #-}

encode :: (MonadIO m, HasAvroSchema a, ToAvro a)
  => SchemaRegistry
  -> Subject
  -> a
  -> m (Either EncodeError ByteString)
encode sr subj a = encodeWithSchema sr subj (schemaOf a) a
{-# INLINE encode #-}

-- | Encodes a provided value into Avro
-- and registers value's schema in SchemaRegistry.
encodeWithSchema :: forall a m. (MonadIO m, ToAvro a)
  => SchemaRegistry
  -> Subject
  -> Schema
  -> a
  -> m (Either EncodeError ByteString)
encodeWithSchema sr subj sch a = do
  mbSid <- sendSchema sr subj sch
  case mbSid of
    Left err  -> return . Left . EncodeRegistryError $ err
    Right sid -> return . Right $ appendSchemaId sid (A.encodeValueWithSchema sch a)


appendSchemaId :: SchemaId -> ByteString -> ByteString
appendSchemaId (SchemaId sid) bs =
  -- add a "magic byte" followed by schema id
  BL.cons (toEnum 0) (B.encode sid) <> bs
