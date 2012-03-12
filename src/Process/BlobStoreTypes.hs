{-# LANGUAGE GeneralizedNewtypeDeriving
           , BangPatterns
  #-}
module Process.BlobStoreTypes
       ( Message (..)
       , ID(..)
       )
       where

import Process

import qualified Data.Serialize as Ser

import Data.UUID
import Crypto.Flat (Skein256)

import Data.ByteString (ByteString)
import Data.Word

-- ID
type Index = Word32

newtype ID  = ID { unID :: (UUID, Index) }
              deriving (Show, Eq)

instance Ser.Serialize ID where
  put = Ser.put . unID
  get = ID `fmap` Ser.get

-- Message
type Value   = ByteString
type Hash    = Skein256
data Message
  = Store !Hash !Value (ID -> IO ())
  | Retrieve !ID !(Reply Value)
