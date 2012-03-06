{-# LANGUAGE TupleSections
           , BangPatterns
  #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.BTree.HashTable.STM
-- A hashtable in STM.
--
-----------------------------------------------------------------------------


module Data.BTree.HashTable.STM
       ( HashTableSTM
       , newSized
       , insert
       , lookup
       , delete
       , size
       , keys
       , toList
       ) where

import Prelude hiding (lookup)

import Control.Concurrent.STM
import Control.Monad

import Data.Array
import Data.Hashable (Hashable(..))
import Data.Maybe

import qualified Data.List as L
import qualified Data.Map  as M


type Bucket k v = M.Map k v


data HashTableSTM k v = HashTableSTM { buckets  :: !Int     -- number of buckets
                                     , count    :: TVar Int -- current element count
                                     , getArray :: Array Int (TVar (Bucket k v)) }


bucket (HashTableSTM n _ arr) k = arr ! (hash k `mod` n)
{-# INLINE bucket #-}

-- | Create a new HashTable with fixed size
newSized :: Int -> IO (HashTableSTM k v)
newSized n =
  do co <- newTVarIO 0
     vs <- mapM (\i -> (i,) `fmap` newTVarIO M.empty) [0..n-1]
     return $! HashTableSTM n co $! array (0, n-1) vs


modifyCount h f = do
  n <- readTVar $ count h
  writeTVar (count h) $! f n
{-# INLINE modifyCount #-}


-- | /O(log n)/. Insert a key/value pair into HashTable.
insert :: (Hashable k, Ord k) => HashTableSTM k v -> k -> v -> STM ()
insert h !k !v = do
     m <- readTVar b
     let m' = M.insert k v m
     writeTVar b $! m'
     modifyCount h (+1)
  where
    b = bucket h k


-- | /O(log n)/. Lookup a key in HashTable.
lookup :: (Hashable k, Ord k) => HashTableSTM k v -> k -> STM (Maybe v)
lookup h !k =
  do lst <- readTVar $ bucket h k
     return $! M.lookup k lst


-- | /O(log n)/. Delete a key from HashTable.
delete :: (Hashable k, Ord k) => HashTableSTM k v -> k -> STM ()
delete h !k =
  do m <- readTVar b
     let (a, m') = M.updateLookupWithKey (\_ _ -> Nothing) k m
     when (isJust a) $
       modifyCount h (subtract 1)
     M.lookup k m' `seq` writeTVar b m'
  where
    b = bucket h k


-- | /O(1)/. Grab the size of the hash table.
size :: HashTableSTM k v -> STM Int
size h = readTVar c
  where
    c = count h


-- | /O(n)/. Get a list of key/value pairs.
toList :: HashTableSTM k v -> STM [(k, v)]
toList h =
  do bs <- buckets h
     return $ concat $ map M.toList bs
  where
    buckets (HashTableSTM n _ arr) =
      mapM readTVar $ elems arr


-- | /O(n)/. Get a list of keys.
keys :: HashTableSTM k v -> STM [k]
keys h =
  do l <- toList h
     return $ map fst l