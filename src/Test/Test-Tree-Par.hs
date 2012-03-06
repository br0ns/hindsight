module Main where

import qualified Data.List as L

import System.Random
import System.Random.Shuffle

import Data.BTree.Cache.Class as Cl
import Data.BTree.Cache.STM as C
import Data.BTree.KVBackend.Files as F
import Data.BTree.Types as Ty
import Data.BTree.BTree as T

import Control.Monad
import Control.Monad.Trans

import Control.Concurrent
import Control.Concurrent.STM

-- tree :: MonadIO m => BTreeM m (C.Param (Ty.Ref Int) Int) Int Int ()
instance Interval Int where
  between a b = (a + b) `div` 2

wModify p (lst, t) =
  do execTree p $ mapM_ (\k -> T.insert k k) lst
     atomically $ putTMVar t 1

wLookup p (lst, t) =
  do n <- execTree p $ foldM (\n k -> do Just m <- T.lookup k
                                         return $! n + m) 0 lst
     atomically $ putTMVar t n

run w ls =
  do tvs <- replicateM (length ls) newEmptyTMVarIO
     mapM_ w $ zip ls tvs
     foldM (\n tv -> do x <- atomically $ readTMVar tv
                        return $! n + x
           ) 0 tvs



main = do
  g <- newStdGen
  let n = 100000 :: Int
      m = 1
      l = split (n `div` 8) $ shuffle' [1..n] n g
  c <- C.sizedParam 128 $ F.evalFilesKV "nodes"

  let root = Nothing -- (Just $ Ty.Ref 0)
  p <- T.makeParam 64 root c

  reb <- T.rebalanceProcess p
  syn <- newEmptyMVar
  pid <- forkIO $ forever $ do withMVar syn $ const $ execTree p $ T.save
                               threadDelay $ 10^6
  putMVar syn pid

  n <- run (wModify p) l
  print n
  -- m <- run (wLookup p) l
  -- print m

  mapM_ (flip withMVar killThread) [reb, syn]

  -- c <- C.sizedParam 128 $ F.evalFilesKV "nodes"
  -- p <- makeParam 64 (Just r) c
  -- l <- execTree p $ T.toList
  -- print $ length l

  where
    nines (a:_, b:_)
      | a == b    = a == '9'
      | otherwise = a == '9' || b == '9'
    split n [] = []
    split n ls = next : split n tl
      where
        (next, tl) = splitAt n ls
