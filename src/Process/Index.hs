{-# LANGUAGE ScopedTypeVariables
           , FlexibleContexts
           , ConstraintKinds
           , RankNTypes
           , ExistentialQuantification
           , BangPatterns
  #-}
module Process.Index
       ( Message (..)
       , index
       )
       where

import Prelude hiding (lookup)

import System.Directory
import System.FilePath

import Util ( safeKill, safeForkIO,
              decode', safeWriteFileWith, safeReadFileWith)

import Channel
import Process
import Supervisor

import Control.Monad.ST
import Control.Monad.State as ST
import Control.Concurrent

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Base64 as B64
import Data.Pickle

import Data.Maybe
import Data.List ((\\))

import qualified Data.BTree.Cache.STM as Cache
import qualified Data.BTree.Class as C
import qualified Data.BTree.BTree as T
import qualified Data.BTree.KVBackend.Files as Back
import Data.BTree.KVBackend.Class (KVBackend)
import Data.BTree.Types (Ref, Node)



data Message k v
  = Insert !k !v
  | Delete !k
  | Lookup !k !(Reply (Maybe v))
  | Modify (v -> v -> v) !k !v !(Reply (Maybe v))
  | Modify_ (v -> v -> v) !k !v
  | ToList !(Reply [(k, v)])
  | Search ((k, k) -> Bool) !(Reply [(k, v)])
  | Flush
  | forall a. Foldli (a -> k -> v -> a) !a !(Reply a)
  | forall a. Foldri (k -> v -> a -> a) !a !(Reply a)
  | forall m. Monad m => Mapi_ (k -> v -> m ()) !(Reply (m ()))


cacheSize = 16
treeOrder = 128

index :: (T.Key k, T.Value v, T.TreeBackend m k v) =>
         FilePath -> (forall a. m a -> IO a) -> ConfigP -> Process (Message k v)
index dir runBackend = newM (hSup, hMsg, init, hFlush, run)
  where
    init = do
      liftIO $ createDirectoryIfMissing True dir
      info "Started"

    hSup m =
      case m of
        Stop -> do info "Shutting down.."
                   (_, _, pids) <- get
                   liftIO $ mapM_ safeKill pids
                   info "Goodbye."
        _    -> warning $ "Ignoring superviser message: " ++ show m

    hMsg m = do
      case m of
        Modify f k v rep ->
          do v <- withTree $ T.modify f k v
             replyTo rep v
        -- Modify f k v rep -> replyTo rep Nothing

        Modify_ f k v -> withTree $ void $ T.modify f k v

        Insert k v -> void $! withTree $! T.modify const k v
        -- Insert k v -> return ()

        Delete k     -> void $ withTree $ T.delete k
        Lookup k rep -> replyTo rep =<< (withTree $! T.lookup k)
        -- Lookup k rep -> replyTo rep Nothing

        ToList rep -> replyTo rep =<< (withTree T.toList)
        Search f rep -> replyTo rep =<< (withTree $ T.search f)

        Flush -> hFlush

        Foldli f a rep -> replyTo rep =<< (withTree $ T.foldli f a)
        Foldri f a rep -> replyTo rep =<< (withTree $ T.foldri f a)
        Mapi_ f rep -> replyTo rep =<< (withTree $ (T.foldli (\a k v -> a >> f k v)) $ return ())

    hFlush = do (p, l, _) <- get
                liftIO $ saveIO p l

    -- flush and save
    saveIO p l = do
      -- take lock
      takeMVar l
      -- save root
      r <- T.execTree p T.save
      safeWriteFileWith id rootFile $ dumps r
      -- release lock
      putMVar l ()

    -- run the tree monad (with root if present, else init new)
    run m = do
      -- setup tree
      root <- getRoot rootFile
      -- c <- Cache.sizedParam cacheSize $ Back.evalFilesKV dir
      c <- Cache.sizedParam cacheSize runBackend
      p <- T.makeParam treeOrder root c
      -- create lock
      lock <- newMVar ()
      -- start background processes
      mv0 <- T.rebalanceProcess p -- TODO: rebalance needs a lock?
      mv1 <- safeForkIO $ \mv -> forever $ do threadDelay $ 10 * 10^6
                                              withMVar mv $ const $ saveIO p lock
      mv2 <- safeForkIO $ \mv -> forever $ do threadDelay $ 10 * 10^6
                                              withMVar mv $ const $ Cache.flush c

      -- run!
      void $ runStateT m (p, lock, [mv2, mv0, mv1])

    withTree m = do (p, _, _) <- get
                    liftIO $ T.execTree p m

    rootFile = dir </> "root"

    getRoot path =
      do exists <- doesFileExist path
         if exists
           then do root <- safeReadFileWith id path
                   Just `fmap` (return $ decode' "Index: Root" root)
           else return Nothing
