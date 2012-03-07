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

import qualified Process.Index as Idx
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
  = Store !Hash !Value
  | Retrieve !ID !(Reply Value)

blobStore workdir minBlobSize hiCh extCh =
  newM (hSup, hMsg, info "Started", hFlush, run)
  where
    run m = do blobId <- uuid
               void $ runStateT m (blobId, [])

    flush =
      do (blobId, blob') <- get
         let blob = L.reverse blob'
             callback = liftIO $ do
               mapM_ (\((hash, _), idx) -> do
                         sendReply hiCh $ Idx.Modify
                           (\_ Nothing -> Just $! ID (blobId, idx))
                           hash
                           (Just $! ID (blobId, idx))
                     ) $ zip blob [0..]
               -- This line is expensive, but might be a good idea if the system crashes often...
               -- sendBlock hiCh Idx.Flush
               unlock
             lockfile = workdir </> byteStringToFileName (encode blobId)
             lock = liftIO $ safeWriteFileWith id lockfile $ encode $ map fst blob
             unlock = removeFile lockfile
         length <- size
         info $ "Flushing: " ++ show length
         unless (L.null blob) $ do
           lock
           send extCh $ Ext.PutCallback
             callback
             (encode blobId)
             (encode $ map snd blob)
         blobId' <- liftIO uuid
         put (blobId', [])
         info "Done!"

    hSup Stop = info "Goodbye."
    hSup m = warning $ "Ignoring supervisor messages." ++ show m

    hMsg msg = {-# SCC "blobStore" #-}
      do blobId <- gets fst
         case msg of
           Store hash v ->
             do store hash v
                -- flush?
                offset <- fmap fromIntegral size
                when (fromIntegral offset + B.length v >= minBlobSize)
                  flush
           Retrieve (ID (blobId, idx)) rep ->
             do blob <- sendReply extCh $ Ext.Get $ encode blobId
                replyTo rep $! (!! (fromIntegral idx)) $ either error id $ decode blob
      where
        store hash v = do modify $ \(blobId, blob) -> (blobId, (hash, v) : blob)

    size    = do (_, blob) <- get
                 return $ L.foldl' (\l c -> l + (B.length $ snd c)) 0 blob

    hFlush = do
      flush
      flushChannel extCh
