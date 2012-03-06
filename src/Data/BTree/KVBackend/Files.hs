{-# LANGUAGE FlexibleInstances
           , MultiParamTypeClasses
           , GeneralizedNewtypeDeriving
           , UndecidableInstances
           , ScopedTypeVariables
  #-}

module Data.BTree.KVBackend.Files where

import Debug.Trace

import Prelude hiding (catch)

import Control.Exception
import Control.Concurrent
import Control.Monad.Reader

import System.Random
import System.FilePath
import System.Directory (removeFile, renameFile)

import Data.Serialize (Serialize, encode, decode)

import qualified Data.ByteString.Char8  as B
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Lazy   as BL
import Data.Word
import Util (atomicFileWrite, safeReadFile, safeWriteFile)

import qualified Data.BTree.KVBackend.Class as KV
import Codec.Compression.Snappy

type Param = FilePath

newtype FilesKV a = FilesKV { runFilesKV :: ReaderT Param IO a }
                      deriving (Monad, MonadIO, MonadReader Param)

evalFilesKV :: FilePath -> FilesKV a -> IO a
evalFilesKV p m =
  runReaderT (runFilesKV m) p

traceThis a = traceShow a a

filePath :: Serialize k => k -> FilesKV FilePath
filePath path =
  do dir <- ask
     return $ dir </> (B.unpack $ B.map fix $ B64.encode $ encode path)
  where
    fix '/' = '-'
    fix c   = c


store k v = do path <- filePath k
               e <- liftIO $! try $! safeWriteFile path bin
               case e of
                 Left (e :: IOError) -> do -- File is probably locked
                   liftIO $ threadDelay 1
                   store k v
                 Right _ -> return ()
  where
    bin = compress $ encode v

fetch k = do path <- filePath k
             liftIO $ do bin <- safeReadFile path
                         return $! either (const Nothing) Just $ decode $ decompress bin
               `catch` \(_ :: IOError) -> return Nothing

remove k = do path <- filePath k
              liftIO $ removeFile path
                `catch` \(_ :: IOError) -> return ()


instance (Show k, Serialize k, Serialize v) => KV.KVBackend FilesKV k v where
  store  k v = store k v
  fetch  k   = fetch k
  remove k   = remove k



