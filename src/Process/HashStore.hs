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

import Prelude hiding (catch)

import Util (byteStringToFileName, safeReadFileWith, decode')

import System.Directory
import System.FilePath
import System.Time

import Process
import Supervisor

import Process.HashStoreTypes

import qualified Process.Stats as Stats
import qualified Process.BlobStore as BS
import qualified Process.Index as Idx
import qualified Process.External as Ext

import Control.Monad.State
import Control.Concurrent
import Control.Exception (try, catch, SomeException)

import Data.UUID

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Base64 as B64
import Data.Pickle

import qualified Data.Set as Set
import Data.List ((\\))
import Data.Maybe

import Crypto.Flat (skein256)

defaultColor = '\NUL'

recover rollback statCh idxCh extCh = do
    ei <- try $ doesDirectoryExist rollback
    case ei of
      Left (e :: SomeException) -> return ()
      Right False               -> return ()
      Right True  -> do
        allfiles <- getDirectoryContents rollback
        -- Skip ".", ".." and tmp files "foo.bar"
        let files = filter (not . (elem '.')) allfiles
        unless (null files) $ do
          send statCh $ Stats.Say "  Rolling back dangling hashes"
          forM_ files $ \file -> do
            let path = rollback </> file
            hashes  <- decode' "HashStore.recover" `fmap` safeReadFileWith id path
            alive <- foldM isKnown False hashes
            unless alive $ do
              send extCh $ Ext.Del $ B.pack file
            removeFile $ rollback </> file
      where
        isKnown alive hash | alive     = return alive
                           | otherwise = do
          send statCh $ Stats.SetMessage $ show hash
          ex <- sendReply idxCh $ Idx.Lookup hash
          return $ isJust ex


hashStore mc statCh idxCh bsCh = new (hSup, hMsg, info "Started", hFlush)
  where
    hSup m =
      case m of
        Stop -> info "Goodbye."
        _ -> warning $ "Ignoring superviser message: " ++ show m

    hMsg !(Insert v rep) = do
      -- check cache
      cached <- liftIO $ withMVar mc $ \s -> return $! id `Set.member` s
      if cached then skip else do
        mbx <- sendReply idxCh $ Idx.Lookup id
        case mbx of
          Just _ -> skip
          Nothing -> do
            ok <- liftIO $ modifyMVar mc $ \s -> do
              if id `Set.member` s then
                return (s, False)
                else
                return (Set.insert id s, True)
            if not ok then skip else do
              send bsCh $ BS.Store id v $ insert id
              replyTo rep id
      where
        !id  = skein256 v
        skip = do
          replyTo rep id
          now <- liftIO $ getClockTime
          send statCh $ Stats.Completed now $ fromIntegral $ B.length v
        insert id blob = do
          sendReply idxCh $! Idx.Modify (error "hashStore: exists") id
            (defaultColor, blob)
          liftIO $ modifyMVar mc $ \s -> return $ (Set.delete id s, ())


    hMsg (Lookup id rep) = do
      mbx <- sendReply idxCh $ Idx.Lookup id
      case mbx of
        Just (_, blobId) -> do
          v <- sendReply bsCh $ BS.Retrieve blobId
          if skein256 v == id
            then replyTo rep $ Just v
            else do critical $ "Invalid hash: " ++ show id
                    replyTo rep $ Nothing
        _ -> do
          info "Could not find hash"
          replyTo rep Nothing

    hFlush = do
      -- flush blob store
      flushChannel bsCh
