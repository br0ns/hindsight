{-# LANGUAGE MultiParamTypeClasses
           , GeneralizedNewtypeDeriving
           , FlexibleContexts
           , FlexibleInstances
           , UndecidableInstances
           , BangPatterns
  #-}

module Data.BTree.KVBackend.CachedExternal where

import Prelude hiding (writeFile, map)

import Process (Channel, send, sendReply)
import Util (unseal', decode')

import Control.Monad.Reader

import Data.ByteString hiding (unpack)
import Data.ByteString.Char8 as B8
import Process.KeyStore (Message(..))

import Data.Serialize (Serialize, encode)
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Lazy   as BL

import qualified Data.BTree.KVBackend.Class as C
import Data.BTree.KVBackend.Files (decompress')
import Control.Concurrent

type Param = Channel Message

newtype CachedExternal m a = CachedExternal {
  runCachedExternal :: ReaderT Param m a
  } deriving (Monad, MonadIO, MonadReader Param)


evalCachedExternal :: Param -> CachedExternal m a -> m a
evalCachedExternal p m =
  runReaderT (runCachedExternal m) p

instance (MonadIO m, C.KVBackend m k v,
          Serialize k, Serialize v, Show k) =>
         C.KVBackend (CachedExternal m) k v where
  store k v = CachedExternal $ lift $ C.store k v
  remove k = CachedExternal $ lift $ C.remove k

  fetch k = CachedExternal $ do
    mk <- lift $ C.fetch k
    case mk of
      Just v -> return $ Just v
      Nothing -> do -- try the channel
        ch <- ask
        mbbs <- liftIO $ sendReply ch $ Retrieve $ B8.map fix $ B64.encode $ encode k
        case mbbs of
          Just bs -> do
            let !v = decode' "CachedExternal" $ unseal' $ decompress' bs
            lift $ C.store k v
            return $ Just v
          Nothing -> do
            return Nothing
        where
          fix '/' = '-'
          fix c = c
