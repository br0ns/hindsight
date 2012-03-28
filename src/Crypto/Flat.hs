module Crypto.Flat where

import Data.ByteString.Char8 as B
import qualified Data.ByteString.Base64 as B64

import Crypto.Classes
import Crypto.Skein

import Data.Word
import Data.Serialize
import Data.BTree.BTree (Interval(..))


newtype Skein256 = Skein256 B.ByteString
              deriving (Eq, Ord)

instance Serialize Skein256 where
  put (Skein256 bs) = do
    let len = fromIntegral $ B.length bs :: Word8
    put len
    putByteString bs
  get = do
    len <- get :: Get Word8
    Skein256 `fmap` getByteString (fromIntegral len)

instance Show Skein256 where
  show (Skein256 bs) = B.unpack $ B64.encode bs

instance Interval Skein256 where
  between (Skein256 a) (Skein256 b) = Skein256 $ between a b

skein256 :: B.ByteString -> Skein256
skein256 s = Skein256 $ encode h
  where
    h = hash' s :: Skein_256_256
