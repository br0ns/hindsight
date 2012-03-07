{-# LANGUAGE ScopedTypeVariables
           , BangPatterns
  #-}

-- TODO: Merge this file with BlobStore somehow, they just have a lot in common

module Process.HashStore
       ( Message (..)
       , ID
       , hashStore
       , recover
       )
       where

import Util (byteStringToFileName, safeReadFileWith)

import System.Directory
import System.FilePath
import System.Time

import Process
import Supervisor

import qualified Process.Stats as Stats
import qualified Process.BlobStore as BS
import qualified Process.Index as Idx
import qualified Process.External as Ext

import Control.Monad.State
import Control.Concurrent

import Data.UUID

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Base64 as B64
import Data.Pickle

import Data.List ((\\))
import Data.Maybe
import Text.Read.HT

import Crypto.Flat (skein256, Skein256)

-- import Data.Interned
-- import Data.Interned.ByteString

-- ID
type ID = Skein256

-- Message
type Value   = ByteString
data Message
  = Insert !Value !(Reply ID)
  | Lookup !ID !(Reply (Maybe Value))

recover rollback statCh idxCh extCh = do
  ex <- doesDirectoryExist rollback
  when ex $ do
    allfiles <- getDirectoryContents rollback
    let files = allfiles \\ [".", ".."]
    unless (null files) $ do
      send statCh $ Stats.Say "  Rolling back dangling hashes"
      forM_ files $ \file -> do
        let path = rollback </> file
        hashes <- (either (const []) id . decode) `fmap` safeReadFileWith id path
        forM_ hashes $ \hash -> do
          send statCh $ Stats.SetMessage $ show hash
          send idxCh $ Idx.Delete hash
        flushChannel idxCh
        -- sendBlock extCh $ Ext.Del $ fileNameToByteString file
        removeFile path
      send idxCh $ Idx.Mapi_ f
      flushChannel idxCh
  where
    f hash Nothing = do
      send statCh $ Stats.SetMessage $ show hash
      send idxCh $ Idx.Delete hash
    f hash _ =
      send statCh $ Stats.SetMessage $ show hash :: IO ()

hashStore statCh idxCh bsCh = new (hSup, hMsg, info "Started", hFlush)
  where
    hSup m =
      case m of
        Stop -> info "Goodbye."
        _ -> warning $ "Ignoring superviser message: " ++ show m

    hMsg !(Insert v rep) = do
      let !id = skein256 v
      replyTo rep id
      mbx <- sendReply idxCh $
             Idx.Modify (flip const) id Nothing
      case mbx of
        Just _ -> do
          now <- liftIO $ getClockTime
          send statCh $ Stats.Completed now $ fromIntegral $ B.length v
        Nothing -> send bsCh $ BS.Store id v

    hMsg (Lookup id rep) = do
      mbx <- sendReply idxCh $ Idx.Lookup id
      case mbx of
        Just (Just blobId) -> do
          v <- sendReply bsCh $ BS.Retrieve blobId
          if skein256 v == id
            then replyTo rep $ Just v
            else do critical $ "Invalid hash: " ++ show id
                    replyTo rep $ Nothing
        _ -> do
          info "Could not find hash"
          replyTo rep Nothing

    hFlush = flushChannel bsCh