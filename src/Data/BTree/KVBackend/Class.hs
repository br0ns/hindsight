{-# LANGUAGE MultiParamTypeClasses
           , FunctionalDependencies
  #-}

module Data.BTree.KVBackend.Class
       ( KVBackend(..) )
       where


class Monad m => KVBackend m k v | m -> k, m -> v where
  store  :: k -> v -> m ()
  fetch  :: k ->      m (Maybe v)
  remove :: k ->      m ()
