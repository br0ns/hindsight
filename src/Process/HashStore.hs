{-# LANGUAGE ScopedTypeVariables
           , BangPatterns
  #-}

-- TODO: Merge this file with BlobStore somehow, they just have a lot in common

module Process.HashStore
       ( Message (..)
       , ID
       , hashStore
       , recover
       , cleanup
       )
       where

import Util (byteStringToFileName, safeReadFileWith, decode')

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
import Control.Exception (try, SomeException)

import Data.UUID

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Base64 as B64
import Data.Pickle

import qualified Data.Set as Set
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
    ei <- try $ doesDirectoryExist rollback
    case ei of
      Left (e :: SomeException) -> return Set.empty
      Right False -> return Set.empty
      Right True  -> do
        allfiles <- getDirectoryContents rollback
        -- Skip ".", ".." and tmp files "foo.bar"
        let files = filter (not . ('.' `elem`)) allfiles
        if null files then return Set.empty else do
          send statCh $ Stats.Say "  Rolling back dangling hashes"
          hset <- Set.fromList `fmap` concat `fmap` (
            forM files $ \file -> do
               let path = rollback </> file
               hashes <- decode' "HashStore.recover" `fmap` safeReadFileWith id path
               forM_ hashes $ \hash -> do
                 send statCh $ Stats.SetMessage $ show hash
                 send idxCh $ Idx.Delete hash
               flushChannel idxCh
               -- sendBlock extCh $ Ext.Del $ fileNameToByteString file
               return hashes
            )
          join $ sendReply idxCh $ Idx.Mapi_ f
          flushChannel idxCh
          return hset
  where
    f hash Nothing = do
      send statCh $ Stats.SetMessage $ show hash
      send idxCh $ Idx.Delete hash
    f hash _ =
      send statCh $ Stats.SetMessage $ show hash :: IO ()

cleanup rollback = do
  ei <- try $ doesDirectoryExist rollback
  case ei of
    Left (e :: SomeException) -> return ()
    Right False -> return ()
    Right True  -> do
      allfiles <- getDirectoryContents rollback
      mapM_ remove allfiles
  where
    remove f | f `elem` [".", ".."] = return ()
    remove f = removeFile f


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