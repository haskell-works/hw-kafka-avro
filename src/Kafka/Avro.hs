module Kafka.Avro
( module X
, propagateKeySchema
, propagateValueSchema
) where

import Control.Monad.IO.Class
import Data.ByteString.Lazy

import Kafka.Avro.Decode         as X
import Kafka.Avro.Encode         as X
import Kafka.Avro.SchemaRegistry as X

-- | Registers schema that was used for a given payload against the specified subject as a key shema.
-- It is possible that a given payload doesn't have schema registered against it, in this case no prapagation happens.
propagateKeySchema :: MonadIO m => SchemaRegistry -> Subject -> ByteString -> m (Either SchemaRegistryError (Maybe SchemaId))
propagateKeySchema sr subj = propagateSchema sr (keySubject subj)

-- | Registers schema that was used for a given payload against the specified subject as a value schema.
-- It is possible that a given payload doesn't have schema registered against it, in this case no prapagation happens.
propagateValueSchema :: MonadIO m => SchemaRegistry -> Subject -> ByteString -> m (Either SchemaRegistryError (Maybe SchemaId))
propagateValueSchema sr subj = propagateSchema sr (keySubject subj)

propagateSchema :: MonadIO m
                => SchemaRegistry
                -> Subject
                -> ByteString
                -> m (Either SchemaRegistryError (Maybe SchemaId))
propagateSchema sr subj bs = do
  case extractSchemaId bs of
    Nothing -> return $ Right Nothing
    Just (sid, _) -> do
      mSchema <- loadSchema sr sid
      case mSchema of
        Left (SchemaRegistrySchemaNotFound _) -> return $ Right Nothing
        Left err                              -> return $ Left err
        Right schema                          -> fmap Just <$> sendSchema sr (keySubject subj) schema
