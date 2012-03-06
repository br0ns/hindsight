{-# LANGUAGE GeneralizedNewtypeDeriving
           , TypeSynonymInstances
           , FlexibleInstances
  #-}

module Data.BTree.Types where

import Data.Hashable (Hashable)

import Data.BTree.Cache.Class

import Data.Word

import Data.Serialize
import Data.Map as M
import Data.Hashable

import Control.Concurrent.STM
import Control.Monad.Reader

import System.Random


newtype Ref a = Ref Word64
              deriving (Show, Ord, Eq, Hashable, Num)


instance Serialize (Ref a) where
  put (Ref a) = put a
  get = Ref `fmap` get


data Node k v = Leaf   (M.Map k v)
              | Branch [k] [Ref (Node k v)]
              deriving Eq


instance (Ord k, Serialize k, Serialize v) => Serialize (Node k v) where
  put (Leaf ks) =
    do put (0 :: Word8)
       put ks

  put (Branch ks rs) =
    do put (1 :: Word8)
       put ks
       put rs

  get = do tag <- get :: Get Word8
           case tag of
             0 -> Leaf `fmap` get
             1 -> do ks <- get
                     rs <- get
                     return $ Branch ks rs


data Param st k v = Param {
    order  :: Int
  , root   :: TVar (Ref (Node k v))
  , state  :: st
  , marked :: TChan k
  , unused :: TVar [Ref (Node k v)]
  }


newtype BTreeM m st k v a = BTreeM { runBTreeM :: ReaderT (Param st k v) m a }
                       deriving (Monad, MonadIO, MonadReader (Param st k v), Functor)


