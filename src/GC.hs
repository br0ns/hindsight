module GC where

import qualified Data.Map as Map
import qualified Data.Set as Set

import Process

import qualified Process.Index as Idx
import qualified Process.External as Ext
import Process.BlobStoreTypes

import Data.Pickle

sweep hiCh extCh = do
  (dead, alive) <- sendReply hiCh $ Idx.Foldli go (Map.empty, Set.empty)
  putStrLn $ concat ["Deleting ", show (Map.size dead)
                    , " dead blobs. Leaving ", show (Set.size alive)
                    , " alive."]
  mapM_ (send extCh . Ext.Del . encode) $ Map.keys dead
  flushChannel extCh
  mapM_ (send hiCh . Idx.Delete) $ concat $ Map.elems dead
  where
    go (dead, alive) hash (clr, ID (blobid, _))
      | clr == 'd' && blobid `Set.member` alive = (dead, alive)
      | clr == 'd' = (Map.insertWith (++) blobid [hash] dead, alive)
      | otherwise = (Map.delete blobid dead, Set.insert blobid alive)
