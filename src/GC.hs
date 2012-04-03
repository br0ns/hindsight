module GC where

import qualified Data.Map as Map
import Process

import qualified Data.ByteString as B
import qualified Process.Index as Idx
import qualified Process.External as Ext
import Process.BlobStoreTypes
import Data.List

import Data.Pickle
import Data.Tuple
import Data.Ord (comparing)
import Util

import Control.Monad

minHashPerBlob = 32
minBlobs       =  2

sweep hiCh extCh bsCh = do
  (dead, alive) <- sendReply hiCh $ Idx.Foldli go (Map.empty, Map.empty)
  putStrLn $ concat ["Deleting ", show (Map.size dead)
                    , " dead blobs. Leaving ", show (Map.size alive)
                    , " alive."]
  -- delete dead blobs
  mapM_ (send extCh . Ext.Del . encode) $ Map.keys dead
  flushChannel extCh
  mapM_ (send hiCh . Idx.Delete) $ concat $ Map.elems dead
  flushChannel hiCh

  -- merge small blobs
  let blobs = takeWhile ((< (minHashPerBlob `div` 2)) . length . snd) $
              sortBy (comparing $ length.snd) $ Map.toList alive
  when (length blobs >= minBlobs) $ do
    putStrLn $ concat ["Rewriting ", show (length blobs), " small blobs."]
    merge blobs
    flushChannel bsCh
    flushChannel hiCh
    -- delete blobs now, in case no dead hash
    mapM_ (send extCh . Ext.Del . encode) $ map fst blobs
    flushChannel extCh
    -- run again to remove dead blobs and hashes
    sweep hiCh extCh bsCh
  where
    go (dead, alive) hash (clr, ID (blobid, pos))
      | clr == 'd' && blobid `Map.member` alive = (dead, alive)
      | clr == 'd' = (Map.insertWith (++) blobid [hash] dead, alive)
      | otherwise = (Map.delete blobid dead, alive')
      where
        alive' = Map.insertWith (++) blobid [(hash, pos)] alive

    merge blobs = forM_ blobs $ \(blobid, hs) -> do
      forM_ hs $ \(hash, pos) -> do
        chunk <- sendReply bsCh $ Retrieve $ ID (blobid, pos)
        send bsCh $ Store hash chunk $ callback hash
      where
        callback hash id = do
          send hiCh $ Idx.Insert hash ('\NUL', id)
