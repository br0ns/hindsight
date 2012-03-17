{-# LANGUAGE ScopedTypeVariables #-}

module Backup
       where

import Prelude hiding (catch)

import Util (epoch, decode', expandUser, safeWriteFileWith)
import Config
import Supervisor
import Process
import Process.Stats (Message (..))
import qualified Process.Stats as Stats

import qualified Process.Log as Log
import qualified Channel as Ch
import qualified Process.KeyStore  as KS
import qualified Process.HashStore as HS
import qualified Process.BlobStore as BS
import qualified Process.External  as Ext
import qualified Process.Index     as Idx

import Data.ByteString.Char8 (pack, unpack)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import Data.ByteString (ByteString)

import System.FilePath
import System.Directory
import System.Posix.Files
import System.Unix.Directory (withTemporaryDirectory)
import System.Time
import System.Time.Utils (clockTimeToEpoch)
import System.Posix.Directory hiding (removeDirectory)
import qualified Stat as S


import System.Posix.Files
import System.Posix.Types
import System.IO

import qualified Codec.Archive.Tar as Tar

import qualified Data.Set as Set
import qualified Data.Map as Map
import Data.Map (Map)

import Control.Concurrent
import Control.Concurrent.STM

import Control.Exception
import Control.Monad.Trans
import Control.Monad.State
import Control.Monad
import Control.Arrow (second, first)

import Data.UUID

import Data.Conduit hiding (Stop)
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Extra as CE

import Data.Function
import Data.List hiding (group)
import Data.Maybe
import Data.Ord

import Crypto.Simple (newMasterKey, readMasterKey)

import Data.Serialize (Serialize(..), encode)
import qualified Data.Serialize as Ser

import qualified Data.BTree.KVBackend.Files as Back
import qualified Data.BTree.KVBackend.CachedExternal as CachedExt

instance Serialize ClockTime where
  put (TOD a b) = Ser.put (a, b)
  get = do (a, b) <- Ser.get
           return $ TOD a b

data Snapshot =
  Snapshot
  { timestamp :: ClockTime
  , reference :: ByteString}
  deriving (Eq)

instance Serialize Snapshot where
  put (Snapshot t r) = Ser.put (t, r)
  get = do (t, r) <- Ser.get
           return $ Snapshot t r

maxBlob = 2 * (1024 ^ 2)

localIndex dir = Idx.index dir $ Back.evalFilesKV dir
remoteIndex extCh dir =
  Idx.index dir $ Back.evalFilesKV dir . CachedExt.evalCachedExternal extCh

backend statCh base mbbufdir = do
  masterKey <- either error id `fmap` (readMasterKey $ base </> "conf" </> "key")
  mod <- expandUser backendModule
  let extP = Ext.external statCh masterKey mbbufdir mod $
             defaultConfigP { name = "external",
                              channelSize = 10 }
  return $ replicateB 5 extP -- TODO: parameterise

stats = return $ Stats.stats 100000 30 defaultConfigP { name = "stats" }

snapP base = localIndex (base </> "snap") $
             defaultConfigP { name = "snapshots" }

init base = do
  createDirectoryIfMissing True $ base </> "conf"
  newMasterKey $ base </> "conf" </> "key"

listall base = do
  with (snapP base) $ \ch -> do
    xs <- sendReply ch $ Idx.ToList
    forM_ (filter (not . B.isPrefixOf (pack "__") . fst) xs) $
      \(repo, snaps :: Map.Map Int Snapshot) -> do
        putStrLn $ unpack repo ++ ":"
        forM_ (reverse $ Map.toList snaps) $ \(num, snap) ->
          putStrLn $ "  " ++ show num ++ ": " ++ show (timestamp snap)

recordSnapshot snapCh extCh name dir = do
  tarball <- tar dir
  now <- getClockTime
  id <- encode `fmap` liftIO uuid
  let snapshot = Snapshot now id
  send extCh $ Ext.Put id tarball
  send snapCh $ Idx.Modify_
    (\_ m -> Map.insert ((1 :: Int) + (fst $ Map.findMax m)) snapshot m)
    key
    $ Map.singleton 1 snapshot
  where
    key = pack name

seal base = do
  let pri  = base </> "pri"
      sec  = base </> "sec"
      snap = base </> "snap"
  statP <- stats
  with statP $ \statCh -> do
    extP <- backend statCh base Nothing
    with extP $ \extCh -> do
      with (snapP base) $ \snapCh -> do
        let record name repo = recordSnapshot snapCh extCh name $ sec </> repo
        send statCh $ Say "Saving index"
        goSnapshot statCh extCh sec "pidx" $ pri </> "idx"
        send statCh $ ClearGoal
        send statCh $ Say "Transferring tarballs"
        -- Pri index
        record "__pidx" "pidx"
        -- Sec index
        record "__sidx" "idx"
        -- Seal
      snapshots <- tar snap
      send extCh $ Ext.Put (pack "snapshots") snapshots

recover base = do
  let pri      = base </> "pri"
      sec      = base </> "sec"
      pidx     = pri </> "idx"
      pidxrepo = sec </> "pidx"
      sidx     = sec </> "idx"
      snap     = base </> "snap"
  forM_ [pri, sec, snap] $ \dir -> do
    ex <- doesDirectoryExist dir
    when ex $ removeDirectoryRecursive dir
    createDirectoryIfMissing True dir
  withTemporaryDirectory "hindsight" $ \tmp -> do
    statP <- stats
    with statP $ \statCh -> do
      extP <- backend statCh base $ Just tmp
      with extP $ \extCh -> do
        snapshots <- sendReply extCh $ Ext.Get $ pack "snapshots"
        untar statCh snap snapshots
        with (snapP base) $ \snapCh -> do
          (mbsidx :: Maybe (Map Int Snapshot)) <- sendReply snapCh $ Idx.Lookup $ pack "__sidx"
          let sidxid = maybe (error "No secondary index") (reference.snd.Map.findMax) mbsidx
          sidxtar <- sendReply extCh $ Ext.Get sidxid
          untar statCh sidx sidxtar
          mbpidx <- sendReply snapCh $ Idx.Lookup $ pack "__pidx"
          let pidxid = maybe (error "No primary index") (reference.snd.Map.findMax) mbpidx
          pidxtar <- sendReply extCh $ Ext.Get pidxid
          untar statCh pidxrepo pidxtar
        send statCh $ Say "Downloading index"
        goCheckout statCh extCh sec "pidx" Nothing pidx

snapshot base name path = do
  let pri      = base </> "pri"
      sec      = base </> "sec"
      repo     = name ++ "~head"
  statP <- stats
  with statP $ \statCh -> do
    extP <- backend statCh base Nothing
    with extP $ \extCh -> do
      send statCh $ Say "Taking snapshot"
      goSnapshot statCh extCh pri repo path
      send statCh $ Say "Saving internal state"
      goSnapshot statCh extCh sec repo $ pri </> repo
      with (snapP base) $ \snapCh ->
        recordSnapshot snapCh extCh name $ sec </> repo

tar dir = do
  entries <- Tar.pack dir ["."]
  -- putStrLn $ Tar.entryPath $ head entries
  return $ B.concat $ BL.toChunks $ Tar.write entries

untar statCh path tarball = do
  -- send statCh $ Say $ "Unpacking tarball to " ++ path
  Tar.unpack path $ Tar.read $ BL.fromChunks [tarball]

list rec = inspect $ const $ goList rec
listdir = inspect $ const goListDir
search = inspect $ const goSearch
checkout rec base name version mbdir dest =
  inspect (\e s k base repo mbdir ->
            goCheckout' rec k s e base repo mbdir dest) base name version mbdir

deleteSnapshot base repo version = do
  with (snapP base) $ \snapCh -> do
    x <- sendReply snapCh $ Idx.Lookup key :: IO (Maybe (Map.Map Int Snapshot))
    case x of
      Nothing -> putStrLn $ "Not found: " ++ repo
      Just m  | version `Map.member` m ->
        do send snapCh $ Idx.Insert key $ Map.delete version m
           flushChannel snapCh
           putStrLn $ "Deleted: " ++ repo ++ "~" ++ show version
              | otherwise -> putStrLn "what"
    where
      key = pack repo


withCacheDirectory base f = do
  let cache = base </> "cache"
  createDirectoryIfMissing True cache
  forkIO $ cleaner cache
  f cache
  where
    cleaner cache = return ()
    -- cleaner path = forever $ do threadDelay $ 5 * 10^6
    --                               all <- getDirectoryContents path
    --                               `catch` \(e :: IOError) -> return []
    --                             let fs = filter (not . (extSeparator `elem`)) all
    --                             mapM_ removeFile (map (path </>) fs)
    --                               `catch` \(e :: IOError) -> return ()


inspect go base name version mbdir = do
  let pri      = base </> "pri"
      sec      = base </> "sec"
      snap     = base </> "snap"
  with (snapP base) $ \snapCh -> do
    mbsnaps <- sendReply snapCh $ Idx.Lookup $ pack name :: IO (Maybe (Map.Map Int Snapshot))
    case mbsnaps of
      Nothing -> putStrLn $ "No snapshot named " ++ name
      Just snaps -> do
        if not (version `Map.member` snaps) || version < 0
          then putStrLn $ "No such snapshot version"
          else do
          let snap = snaps Map.! version
              snapref = reference snap
              repo = name ++ "~" ++ show (clockTimeToEpoch $ timestamp snap)
          withCacheDirectory base $ \tmp -> do
            statP <- stats
            with statP $ \statCh -> do
              extP <- backend statCh base $ Just tmp
              with extP $ \extCh -> do
                initDir (sec </> repo) $ \d -> do
                  tarball <- sendReply extCh $ Ext.Get snapref
                  untar statCh d tarball
                initDir (pri </> repo) $ \d -> do
                  goCheckout statCh extCh sec repo (Just "root") d

                cache <- newMVar Set.empty
                let hiP  = localIndex (sec </> "idx") $
                           defaultConfigP { name = "hash index" }
                    kiP         = localIndex (sec </> repo) $
                                  defaultConfigP { name = "key index (" ++ repo ++ ")" }
                    bsP eCh     = BS.blobStore "" maxBlob eCh $
                                  defaultConfigP { name = "blobstore (" ++ repo ++ ")" }
                    hsP hCh bCh = HS.hashStore cache statCh hCh bCh $
                                  defaultConfigP { name = "hashstore (" ++ repo ++ ")" }
                    ksP hCh bCh = KS.keyStore "" hCh bCh $
                                  defaultConfigP { name = "keystore (" ++ repo ++ ")" }
                with hiP $ \hiCh ->
                  with kiP $ \kiCh ->
                  with (ksP kiCh -|- hsP hiCh -|- bsP extCh) $ \ksCh -> do
                    let kiP = remoteIndex ksCh (pri </> repo) $
                              defaultConfigP { name = "key index (" ++ repo ++ ")" }
                    with kiP $ \(kiCh :: Channel KIMessage) -> do
                      go extCh statCh kiCh pri repo mbdir
  where
    initDir d k = do
      ex <- doesDirectoryExist d
      unless ex $ do
        createDirectoryIfMissing True d
        k d

goSnapshot statCh extCh base repo path = do
  let idx       = base </> "idx"
      repodir   = base </> repo
      irollback = idx </> "rollback"
      rrollback = repodir </> "rollback"
  mapM_ (createDirectoryIfMissing True)
    [idx, repodir]
  (wsup, rsup) <- Ch.newUnboundedChannelP
  let spawn' = spawn $ Just rsup
      hiP  = localIndex idx $
             defaultConfigP { name = "hash index" }
  hiCh <- spawn' hiP
  HS.recover irollback statCh hiCh extCh -- Recover from crash if necessary

  cache <- newMVar Set.empty
  let kiP         = localIndex repodir $
                    defaultConfigP { name = "key index (" ++ repo ++ ")" }
      bsP eCh     = BS.blobStore irollback maxBlob eCh $
                    defaultConfigP { name = "blobstore (" ++ repo ++ ")" }
      hsP hCh bCh = HS.hashStore cache statCh hCh bCh $
                    defaultConfigP { name = "hashstore (" ++ repo ++ ")" }
      ksP hCh bCh = KS.keyStore rrollback hCh bCh $
                    defaultConfigP { name = "keystore (" ++ repo ++ ")" }
  kiCh <- spawn' kiP
  KS.recover rrollback statCh kiCh hiCh
  -- take "locks"
  mapM_ (createDirectoryIfMissing True)
    [irollback, rrollback]
  -- let's do this
  ksCh <- spawn' $ replicateB 3 -- TODO: paramaterise
          (ksP kiCh -|- hsP hiCh -|- bsP extCh)
  removeMissing kiCh
  send statCh $ Say "  Calculating size"
  totSize <- runResourceT $ traverse statCh path $$ sumFileSize
  send statCh $ Say "  Transferring"
  send statCh $ SetGoal $ fromIntegral totSize
  -- start cleaner
  pid <- forkIO $ rollbackCleaner rrollback ksCh kiCh hiCh
  -- insert keys!
  runResourceT $ traverse statCh path $= CE.group 1024 $$
    CL.mapM_ (sendFiles rrollback ksCh)
  -- stop cleaner
  killThread pid
  flush ksCh kiCh hiCh
  -- release "locks"
  mapM_ removeDirectoryRecursive
    [irollback, rrollback]
  Ch.sendP wsup Stop
  where
    flush ksCh kiCh hiCh = do
        flushChannel ksCh -- Flush from chain to trees and externally
        flushChannel kiCh -- Flush key tree to disk
        flushChannel hiCh -- Flush hash tree to disk

    rollbackCleaner dir ksCh kiCh hiCh = forever $ do
                                         threadDelay $ (10^6) * flushInterval
                                         go
      where
        go = do
          now <- epoch
          flush ksCh kiCh hiCh
          files <- filter (not . (elem '.')) `fmap` getDirectoryContents dir
          forM_ files $ \file -> do
            let path = dir </> file
            mt <- modificationTime `fmap` getFileStatus path
            when (fromEnum mt < fromIntegral now) $ do
              removeFile path

    toKey = pack . makeRelative path

    sendFiles dir ksCh lst = do
      uid <- show `fmap` uuid
      safeWriteFileWith id (dir </> uid) $ encode $ map (toKey.fst) lst
      mapM_ (sendFile ksCh) lst

    sendFile ksCh (file, stat) = do
      let modtime = modificationTime stat
      rep <- liftIO newEmptyTMVarIO
      liftIO $ send ksCh $
        KS.Insert
        (Just $ encode $ fromEnum modtime)
        (toKey file)
        (encode `fmap` S.readPosixFile (Just stat) file)
        (if isRegularFile stat then KS.File file
         else KS.None)
        rep
      void $ liftIO $ forkIO $
        do x <- atomically $ readTMVar rep
           case x of
             Left _    -> return ()
             Right ins -> unless ins $ do
               now <- getClockTime
               send statCh $ Stats.Completed now $ fromIntegral $ fileSize stat
    removeMissing kiCh = do
      void $ join $ sendReply kiCh $ Idx.Mapi_ $ \file _ -> do
        (void . getSymbolicLinkStatus) (path </> B8.unpack file)
          `catch` \(e :: SomeException) -> do
            send statCh $ SetMessage $ B8.unpack file
            send kiCh $ Idx.Delete file

sumFileSize = CL.fold (\n (_, stat) -> n + fileSize stat) 0

traverse statCh path =
  Source
  { sourcePull = do
       stream <- liftIO $ openDirStream path
       let state = [(path, stream)]
       pull state
  , sourceClose = return ()
  }
  where
    src state = Source (pull state) (return ())
    pull state = do
      res <- pull0 state
      return $ case res of
        StateClosed ->  Closed
        StateOpen state' val -> Open (src state') val
    pull0 [] = return StateClosed
    pull0 all @ ((path, stream) : rest) = do
      -- liftIO $ print path
      entry <- liftIO $ readDirStream stream
      case entry of
        ".." -> pull0 all
        "."  -> pull0 all
        ""   -> do
          liftIO $ closeDirStream stream
          pull0 rest
        _    -> do
          let path' = path </> entry
          send statCh $ SetMessage path'
          estat <- liftIO $ try $ getSymbolicLinkStatus path'
          case estat of
            Left  (e :: SomeException) -> do
              liftIO $ Log.warning "Traverse" $
                "Skipping file: " ++ path ++ " -- " ++ show e
              pull0 all
            Right stat
              | isRegularFile stat -> return $ StateOpen all (path', stat)
              | isDirectory   stat -> do
                  estream' <- liftIO $ try $ openDirStream path'
                  case estream' of
                    Left (e :: SomeException) -> do
                      liftIO $ Log.warning "Traverse" $
                        "Skipping dir: " ++ path' ++ " -- " ++ show e
                      pull0 all
                    Right stream' ->
                      return $ StateOpen ((path', stream') : all) (path', stat)
              | isSymbolicLink stat -> return $ StateOpen all (path', stat)
              | otherwise           -> pull0 all

type KIMessage = Idx.Message ByteString (Maybe ByteString, HS.ID, [HS.ID])

goInspect recursive go mbterm kiCh = do
  case mbterm of
    Just x' -> do
      if x /= "" then do
        mbv <- sendReply kiCh $ Idx.Lookup key
        case mbv of
          Just v -> do
            typ <- S.fileType `fmap` go (key, v)
            when (typ == S.Directory) goDir
          Nothing -> putStrLn "No such file or directory"
        else goDir
      where
        goDir = do
          kvs <- sendReply kiCh $ Idx.Search search
          mapM_ go kvs
        x = if length x' > 0 && last x' == '/'
            then Data.List.init x'
            else x'
        search =
          if recursive
          then searchRec
          else searchNoRec
        searchRec (min, max) =
          min <= key' && key' <= max ||
          key' `B.isPrefixOf` min
        searchNoRec (min, max) =
          searchRec (min, max) &&
          (B.all ('/' /=) (B.drop (B.length key') min) || dir min /= dir max)
        dir = B.takeWhile (/= '/') . B.drop (B.length key')
        key = pack x
        key' = if x == ""
               then key
               else pack $ x ++ "/"
    Nothing -> do
      kvs <- sendReply kiCh $ Idx.ToList
      mapM_ go kvs

goCheckout statCh extCh base repo mbdir dest =
  with (localIndex (base </> repo) defaultConfigP { name = "hash index" }) $ \kiCh ->
  goCheckout' True kiCh statCh extCh base repo mbdir dest

goCheckout' rec (kiCh :: Channel KIMessage) statCh extCh base repo mbterm dest = do
  (wsup, rsup) <- Ch.newUnboundedChannelP
  let spawn' = spawn $ Just rsup
      hiP  = localIndex (base </> "idx") $
             defaultConfigP { name = "hash index" }
  hiCh <- spawn' hiP
  cache <- newMVar Set.empty
  let bsP eCh     = BS.blobStore "" maxBlob eCh $
                    defaultConfigP { name = "blobstore (" ++ repo ++ ")" }
      hsP hCh bCh = HS.hashStore cache statCh hCh bCh $
                    defaultConfigP { name = "hashstore (" ++ repo ++ ")" }
      -- ksP hCh bCh = KS.keyStore hCh bCh $
      --               defaultConfigP { name = "keystore (" ++ repo ++ ")" }
  hsCh <- spawn' $ replicateB 3 -- TODO: paramaterise
          (hsP hiCh -|- bsP extCh)
  restore dest kiCh hsCh
  Ch.sendP wsup Stop
    where
      restore dest kiCh hsCh =
        goInspect rec unpackOneSafe mbterm kiCh
          where
            unpackOneSafe x = do
              ei <- try $ unpackOne x
              case ei of
                Left (e :: SomeException) -> do
                  Log.warning "Checkout" $ show e
                  return undefined
                Right v -> return v

            unpackOne (key, (_, metahash, hashes)) = do
              hFlush stdout
              let path = dest </> B8.unpack key
                  dir  = takeDirectory path
              -- Create dir and preserve it's timestamps if already exist
              createDirectoryIfMissing True dir
              dirStat <- S.readPosixFile Nothing dir
              -- Get meta data
              Just metachunk <- sendReply hsCh $ HS.Lookup metahash
              let stat = decode' "Metachunk" metachunk :: S.PosixFile
              -- Create posix file with correct permissions
              case S.fileType stat of
                S.File -> do
                  h <- openFile path WriteMode
                  forM_ hashes $ \hash -> do
                    Just chunk <- sendReply hsCh $ HS.Lookup hash
                    B.hPut h chunk
                  hClose h
                  -- restore timestamps
                  S.updatePosixFile stat path
                _ -> S.createPosixFile stat path
              -- Restore timestamps of parent dir
              S.updatePosixFile dirStat dir
              send statCh $ SetMessage path
              return stat

goList rec statCh kiCh base repo mbterm = do
  kvs <-
    case mbterm of
      Just x ->
        sendReply kiCh $ Idx.Search search
          where
            search (min, max) =
              min <= key && key <= max || key `B.isPrefixOf` min
            key = pack x
      Nothing -> sendReply kiCh $ Idx.ToList
  sendBlock statCh Quiet
  forM_ (sort $ map fst kvs) $ \file ->
    putStrLn $ B.unpack file

goListDir statCh kiCh base repo term = do
  kvs <- sendReply kiCh $ Idx.Search search
  sendBlock statCh Quiet
  forM_ (sort $ map fst kvs) $ \file ->
    putStrLn $ B.unpack file
  where
    search (min, max) =
      (min <= key' && key' <= max ||
       key' `B.isPrefixOf` min) &&
      (B.all ('/' /=) (B.drop (B.length key') min) || dir min /= dir max)
    dir = B.takeWhile (/= '/') . B.drop (B.length key')
    key = pack term
    key' = if term == ""
           then key
           else pack $ term ++ "/"

goSearch statCh kiCh base repo term = do
  kvs <- sendReply kiCh $ Idx.Search $ \(min, max) ->
    min /= max || (pack term `B.isInfixOf` min)
  sendBlock statCh Quiet
  forM_ (sort $ map fst kvs) $ \file ->
    putStrLn $ B.unpack file
