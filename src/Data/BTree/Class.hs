{-# LANGUAGE MultiParamTypeClasses
           , GeneralizedNewtypeDeriving
           , FunctionalDependencies
           , FlexibleInstances
           , FlexibleContexts
           , UndecidableInstances
  #-}

module Data.BTree.Class where

import Prelude hiding (lookup)
import Data.Maybe
import Data.Serialize (Serialize)

import Control.Monad.Trans
import Control.Monad

import Data.BTree.Types
import qualified Data.BTree.BTree as T
import qualified Data.BTree.Cache.Class as C
import qualified Data.BTree.Cache.STM   as Cstm
import qualified Data.BTree.KVBackend.Class as KV

class (Functor m, MonadIO m) => Tree m k v | m -> v, m -> k where
  -- lookup k <- case lookup k of
  --               Nothing -> v
  --               Just v' -> f v v'
  -- modify f k v = Just v'       if T(k) = v'
  --              = Nothing       otherwise
  modify :: (v -> v -> v) -> k -> v -> m (Maybe v)

  modify_ :: (v -> v -> v) -> k -> v -> m ()
  modify_ f k = void . modify f k

  modifyMany :: [(v -> v -> v, k, v)] -> m ()
  modifyMany fks = mapM_ (\(f, k, v)  -> modify_ f k v) fks

  delete  :: k                        -> m (Maybe v)
  delete_ :: k                        -> m ()
  delete_ = void . delete

  lookup :: k                         -> m (Maybe v)

  member :: k                         -> m Bool
  member k = isJust `fmap` lookup k

  search :: ((k, k) -> Bool)          -> m [(k, v)]

  foldli :: (a -> k -> v -> a) -> a -> m a

  toList :: m [(k,v)]
  toList = reverse `fmap` (foldli (\l k v -> (k,v):l) [])


type N k v = Node k v
type R k v = Ref (N k v)

-- Cache Parameter
type Cp m k v = Cstm.Param m (R k v) (N k v)


type S m k v = Cstm.Param m (R k v) (N k v)

