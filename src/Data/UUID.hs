{-# LANGUAGE DeriveDataTypeable
           , GeneralizedNewtypeDeriving
  #-}

module Data.UUID
       ( UUID
       , uuid
       )
       where


import Data.Word
import Text.Printf
import Control.Concurrent
import Data.Typeable

import qualified Data.Char as C
import qualified Crypto.Random.AESCtr as Prng
import System.Entropy

import Data.Serialize

import System.IO.Unsafe
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Base16 as B16

newtype UUID = UUID { unUUID :: B.ByteString }
             deriving (Ord, Eq)


instance Serialize UUID where
  put = put . unUUID
  get = UUID `fmap` get



instance Show UUID where
  show (UUID bs) = B.unpack $ B16.encode bs

instance Read UUID where
  readsPrec _ s' = let s = dropWhile C.isSpace s'
                       (bs, left) = B16.decode $ B.pack $ take 32 s
                  in [(UUID bs, drop 32 s) | B.null left]



uuids :: MVar [UUID]
uuids = unsafePerformIO $ newMVar []
{-# NOINLINE uuids #-}


uuid :: IO UUID
uuid =
  do ls <- takeMVar uuids
     case ls of
       (x:xs) -> do putMVar uuids xs
                    return x
       [] -> do ent <- getEntropy 64
                let Right gen = Prng.make ent
                putMVar uuids $ map UUID $ map fst $
                  {- always skip first -}
                  tail $ iterate (\(_, gen') -> Prng.genRandomBytes gen' 16) (B.empty, gen)
                uuid


