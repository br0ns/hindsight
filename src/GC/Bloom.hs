{-# LANGUAGE ScopedTypeVariables #-}

module GC.Bloom (
    markBloom
  , computeBloom
  )
       where

import Util

import Process
import Process.Index as Idx
import qualified Process.HashStoreTypes as HST

import qualified Data.Map as Map
import Data.BloomFilter
import Control.Monad
import qualified Data.Array.Unboxed as A

import Data.Pickle (encode)
import Data.Bits ((.|.))


computeBloom kiCh = do
  -- compute size
  hashes <- map (\(_, (_, _, hs)) -> hs) `fmap` (sendReply kiCh Idx.ToList)
  -- populate filter
  -- we use 16 bits per hash
  let filter  = createB hashf (16 * length hashes) $ \mb ->
        mapM_ (mapM_ (insertMB mb)) hashes
  return filter


hashf :: HST.ID -> [Hash]
hashf x = [a, b, c, d]
  where
    (_ :: Char, a, b, c, d) = decode' "GC.Bloom: hashf" $ encode x


markBloom hiCh blobs = do
  let filters = map (constructB hashf) $ Map.elems $
                foldl merge Map.empty blobs
  join $ sendReply hiCh $ Idx.Mapi_ $ \hash (clr, val) -> do
    unless (clr == 'd' || hash `usedBy` filters) $ do
      -- mark hash as dead
      send hiCh $ Idx.Insert hash ('d', val)
  where
    merge m blob = Map.insertWith bitor (length assc) arr1 m
      where
        arr1 = decode' "GC: mark" $ unseal' blob :: A.UArray Int Hash
        assc = A.assocs arr1
        bitor _ old = A.accum (.|.) old assc

    hash `usedBy` fs = any (elemB hash) fs