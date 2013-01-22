module Main where

import qualified Data.ByteString.Char8 as B
import qualified Data.List             as L

import Control.Monad
import Control.Monad.Trans
import Data.SuffixTree


testT keys = runNew "/tmp/tree" defaultPrefixer $ do
  forM_ (zip [1..] keys) $ \(i, k) -> do
    when (i `rem` 1000 == 0) $ liftIO $ print i
    insertT k k
    x <- lookupT k
    when (x /= Just k) $ error $
      L.concat ["Invalid result: got ", show x, " but expected ", show k]
  -- printT

main = do
  keys <- B.lines `fmap` B.getContents
  testT keys