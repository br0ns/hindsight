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

import Util (byteStringToFileName, safeWriteFile, safeReadFile)

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

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL


-- ID
type Offset = Int
type Length = Int


newtype ID  = ID { unID :: (UUID, Offset, Length) }
              deriving (Show, Eq)

instance Ser.Serialize ID where
  put = Ser.put . unID
  get = ID `fmap` Ser.get

-- Message
type Value   = ByteString
type Hash    = ByteString
data Message
  = Store !Hash !Value
  | Retrieve !ID !(Reply Value)

blobStore workdir minBlobSize hiCh extCh =
  newM (hSup, hMsg, info "Started", hFlush, run)
  where
    run m = do blobId <- uuid
               void $ runStateT m (blobId, [], BL.empty)

    flush =
      do (blobId, hashids, blob) <- get
         let callback = liftIO $ do
               mapM_ (\(hash, id) -> do
                         sendReply hiCh $ Idx.Modify
                           (\_ Nothing -> Just id)
                           hash
                           (Just id)
                     ) hashids
               -- This line is expensive, but might be a good idea if the system crashes often...
               -- sendBlock hiCh Idx.Flush
               unlock
             lockfile = workdir </> byteStringToFileName (encode blobId)
             lock = liftIO $ safeWriteFile lockfile $ encode $ map fst hashids
             unlock = removeFile lockfile
         info $ "Flushing: " ++ show (BL.length blob)
         unless (BL.null blob) $ do
           lock
           send extCh $ Ext.PutCallback
             callback
             (encode blobId)
             (B.concat $ BL.toChunks blob)
         blobId' <- liftIO uuid
         put (blobId', [], BL.empty)
         info "Done!"

    hSup Stop = info "Goodbye."
    hSup m = warning $ "Ignoring supervisor messages." ++ show m

    hMsg msg = {-# SCC "blobStore" #-}
      do blobId <- gets $ \(x, _, _) -> x
         case msg of
           Store hash v ->
             do offset <- fmap fromIntegral size
                let !id = ID (blobId, offset, B.length v)
                store hash id v
                -- flush?
                when (offset + B.length v >= minBlobSize)
                  flush
           Retrieve (ID (blobId, offset, len)) rep ->
             do blob <- sendReply extCh $ Ext.Get $ encode blobId
                replyTo rep $ B.take (fromIntegral len)
                            $ B.drop (fromIntegral offset) blob
      where
        store hash id v = do let f = flip BL.append $! BL.fromChunks [v]
                             modify $ \(blobId, hashIds, blob) ->
                               (blobId, (hash, id) : hashIds, f blob)
        size    = get >>= (\(_, _, blob) -> return $ BL.length blob)

    hFlush = do
      flush
      flushChannel extCh
