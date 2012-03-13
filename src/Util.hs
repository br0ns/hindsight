{-# LANGUAGE ScopedTypeVariables #-}

module Util
       ( byteStringToFileName
       , fileNameToByteString
       , clearDirectory
       , atomicFileWrite
       , safeWriteFileWith
       , safeReadFileWith
       , seal
       , unseal
       , unseal'
       , safeForkIO
       , safeKill
       , decode'
       , epoch
       , expandUser
       )
       where

import Prelude hiding (catch)

import qualified Data.ByteString.Char8 as B
import Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Base64 as B64
import System.Directory
import Control.Monad

import Data.Serialize (encode, decode)

import Data.UUID
import System.FilePath ((<.>))
import System.Directory (renameFile)

import System.Time       (getClockTime)
import System.Time.Utils (clockTimeToEpoch)

import Control.Concurrent
import Control.Exception

import Crypto.Flat (skein256)

epoch = clockTimeToEpoch `fmap` getClockTime

byteStringToFileName k = map fixChars $ B.unpack $ B64.encode k
  where
    fixChars '/' = '-'
    fixChars c   = c

fileNameToByteString f = unRight $ B64.decode $ B.pack $ map unfixChars f
  where
    unRight (Right x) = x
    unfixChars '-' = '/'
    unfixChars c   = c

clearDirectory path = do
  metaExists <- doesDirectoryExist path
  when metaExists $
    removeDirectoryRecursive path
  createDirectoryIfMissing True path


atomicFileWrite path bytes = do
  tmp <- (path <.>) `fmap` show `fmap` uuid
  write tmp
  where
    writeThenMove tmp = do
      B.writeFile tmp bytes
      renameFile tmp path

    write tmp = do
      writeThenMove tmp `finally` (removeFile tmp `catch` \(_::IOError) -> return ())

safeWriteFileWith f path bytes = atomicFileWrite path $ f $ seal bytes
safeReadFileWith  f path       = unseal' `fmap` f `fmap` B.readFile path


seal :: B.ByteString -> B.ByteString
seal bytes = encode (hash, bytes)
  where hash = skein256 bytes


unseal' :: B.ByteString -> B.ByteString
unseal' bin = either error id $ unseal bin

unseal :: B.ByteString -> Either String B.ByteString
unseal bin
  | Right (hash', bytes) <- decode bin,
    skein256 bytes == hash' = Right bytes
  | otherwise = Left "unseal: invalid checksum"


safeForkIO f = do
  mv  <- newEmptyMVar
  pid <- forkIO $ f mv
  putMVar mv pid
  return mv


safeKill m = do
  withMVar m killThread


decode' errmsg a =
  either (\msg -> error $ errmsg ++ ": " ++ msg) id $ decode a

expandUser "~"                = getHomeDirectory
expandUser ('~': p @ ('/':_)) = fmap (++ p) getHomeDirectory
expandUser p                  = return p