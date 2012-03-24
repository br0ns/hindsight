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
import Util

import Control.Monad

minHashPerBlob = 16

sweep hiCh extCh bsCh = do
  (dead, alive) <- sendReply hiCh $ Idx.Foldli go (Map.empty, Map.empty)
  putStrLn $ concat ["Deleting ", show (Map.size dead)
                    , " dead blobs. Leaving ", show (Map.size alive)
                    , " alive."]
  -- delete dead blobs
  mapM_ (send extCh . Ext.Del . encode) $ Map.keys dead
  flushChannel extCh
  mapM_ (send hiCh . Idx.Delete) $ concat $ Map.elems dead

  -- merge small blobs
  mapM_ merge $ takeWhile ((< minHashPerBlob) . length . fst) $
    sortBy (\(a, _) (b, _) -> length a `compare` length b) $
    map swap $ Map.toList alive
  flushChannel bsCh
  where
    go (dead, alive) hash (clr, ID (blobid, pos))
      | clr == 'd' && blobid `Map.member` alive = (dead, alive)
      | clr == 'd' = (Map.insertWith (++) blobid [hash] dead, alive)
      | otherwise = (Map.delete blobid dead, alive')
      where
        alive' = Map.insertWith (++) blobid [(hash, pos)] alive

    merge (hs, blobid) = do
      chunks <- decode' "GC:sweep" `fmap`
                (sendReply extCh $ Ext.Get $ encode blobid)
                :: IO [B.ByteString]
      forM_ hs $ \(hash, pos) -> do
        send bsCh $ Store hash (chunks !! fromIntegral pos) $ callback hash
      where
        callback hash id = do
          send hiCh $ Idx.Modify_ (\(clr, _) _ -> (clr, id)) hash ('\NUL', id)