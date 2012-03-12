{-# LANGUAGE GeneralizedNewtypeDeriving
           , BangPatterns
  #-}
module Process.BlobStore
       ( Message (..)
       , ID
       , blobStore
       )
       where

import Process
import Supervisor

import Util (byteStringToFileName, safeWriteFileWith)

import System.Directory
import System.FilePath

import Process.BlobStoreTypes

import qualified Process.HashStoreTypes as HS
import qualified Process.External as Ext

import Control.Monad
import Control.Monad.Trans
import Control.Monad.State.Strict

import Data.Pickle
import qualified Data.Serialize as Ser

import Data.UUID
import Crypto.Flat (Skein256)

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L

import Data.Word

blobStore workdir minBlobSize extCh =
  newM (hSup, hMsg, info "Started", hFlush, run)
  where
    run m = do blobId <- uuid
               void $ runStateT m (blobId, [])

    flush =
      do (blobId, blob') <- get
         let blob = L.reverse blob'
             callback = liftIO $ do
               mapM_ (\((hash, _, cb), idx) -> cb $! ID (blobId, idx))
                     $ zip blob [0..]
               -- This line is expensive, but might be a good idea if the system crashes often...
               -- sendBlock hiCh Idx.Flush
             lockfile = workdir </> byteStringToFileName (encode blobId)
             lock = liftIO $ safeWriteFileWith id lockfile $ encode $
                    map (\(h, _, _) -> h) blob
         length <- size
         info $ "Flushing: " ++ show length
         unless (L.null blob) $ do
           lock
           send extCh $ Ext.PutCallback
             callback
             (encode blobId)
             (encode $ map (\(_, v, _) -> v) $ blob)
         blobId' <- liftIO uuid
         put (blobId', [])
         info "Done!"

    hSup Stop = info "Goodbye."
    hSup m = warning $ "Ignoring supervisor messages." ++ show m

    hMsg msg = {-# SCC "blobStore" #-}
      do blobId <- gets fst
         case msg of
           Store hash v cb ->
             do store hash v cb
                -- flush?
                offset <- fmap fromIntegral size
                when (fromIntegral offset + B.length v >= minBlobSize)
                  flush
           Retrieve (ID (blobId, idx)) rep ->
             do blob <- sendReply extCh $ Ext.Get $ encode blobId
                replyTo rep $! (!! (fromIntegral idx)) $ either error id $ decode blob
      where
        store hash v cb = do
          modify $ \(blobId, blob) -> (blobId, (hash, v, cb) : blob)

    size    = do (_, blob) <- get
                 return $ L.foldl' (\l (_, v, _) -> l + (B.length v)) 0 blob

    hFlush = do
      flush
      flushChannel extCh
