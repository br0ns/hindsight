module Crypto.Flat where

import Data.ByteString as B
import Crypto.Classes
import Crypto.Skein

import Data.Serialize

skein256 :: B.ByteString -> B.ByteString
skein256 s = encode h
  where
    h = hash' s :: Skein_256_256


