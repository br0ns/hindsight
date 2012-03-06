{-# LANGUAGE MultiParamTypeClasses
           , FunctionalDependencies
           , ExistentialQuantification
  #-}

module Data.BTree.Cache.Class where

import Control.Concurrent.STM


data R b = forall a. Inter (IO a) (a -> STM ())
         | Final b


class Monad m => Cache m p k v | m -> p, p -> k, p -> v, p -> m where
  store  :: Maybe k -> k -> v -> m ()
  fetch  ::            k -> m (Maybe v)
  remove :: Maybe k -> k -> m ()
  sync   :: p ->        IO ()
  eval   :: p -> m a -> IO a