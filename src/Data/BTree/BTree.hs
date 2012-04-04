{-# LANGUAGE FlexibleContexts
           , FlexibleInstances
           , ScopedTypeVariables
           , BangPatterns
           , UndecidableInstances
           , ConstraintKinds
           , Rank2Types
  #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.BTree.BTree
-- Concurrent BTree with relaxed balance.
--
--      This module is inspired by the paper
--      /B-Trees with Relaxed Balance/,
--        Kim S. Larsen and Rolf Fagerberg, 1993
--          Department of Mathematics and Computer Science, Odense University, Denmark.
--
-- This implementation is not full, and has some serious limitations:
--
-- 1. The rebalance logic to handle underful leafs has not been implemented.
--
-- 1. 'toList', 'foldli', 'foldri', 'search', 'findMin' and 'findMax' may fail
-- if run in parallel with 'rebalanceProcess'. The current implementations of
-- these operations are therefore considered unsafe.
--
-- 1. 'findMin' and 'findMax' may fail in the case where the outer leaf is empty.
--
-- It is important to note, that these limitations are limitations of the
-- current implementation and not of the original design. They are solely due to
-- lack of time.
--
-- To clarify: @Safe operations@ are those that support rebalancing during the
-- operations, while @unsafe operations@ may fail if run during rebalancing.
-----------------------------------------------------------------------------


module Data.BTree.BTree
       ( -- * Setup and execution:
         makeParam
       , execTree
         -- * Class and type aliases
       , Key
       , Value
       , Interval(..)
       , TreeBackend
       , TreeResult
         -- * Safe operations:
       , insert
       , delete
       , lookup
       , modify
       , save
         -- * Rebalancing:
       , rebalanceProcess
         -- * Unsafe operations:
       , toList
       , foldli
       , foldri
       , search
       , findMin
       , findMax
       , height
       , listNodes
       ) where

import Data.BTree.Types
import Data.Maybe

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad.Reader
import Control.Monad.Trans
import System.Random (randomIO)


import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.ByteString as B
import Data.Serialize (Serialize)

import qualified Data.BTree.Cache.Class as C
import qualified Data.BTree.Cache.STM   as Cstm
import qualified Data.BTree.KVBackend.Class as KV
-- import GHC.Conc

import Debug.Trace

import Prelude hiding (lookup, catch)


-- | Type aliases to ease reading.
type CacheSTM  m k v = Cstm.CacheSTM m (Ref (Node k v)) (Node k v)
type CacheSTMP m k v = Cstm.Param    m (Ref (Node k v)) (Node k v)


-- | Some type-fu. Context (Key k) gives the context (Ord k, Serialize k, ...)
class (Ord k, Serialize k, Interval k)    => Key k
-- | Dummy instance.
instance (Ord k, Serialize k, Interval k) => Key k


-- | Some type-fu. Context (Value v) gives the context (Eq v, Serialize v, ...)
class (Eq  v, Serialize v)    => Value v
-- | Dummy instance.
instance (Eq  v, Serialize v) => Value v


-- | Needed to generate the split-keys used in branch nodes.
class Interval k where
  -- | Given two keys, a < c, compute a new key b, such that a <= b < c.
  -- Default is to choose a, however a smarter instance exist for ByteString.
  between :: k -> k -> k
  between = const


-- | Instance for Bytestring that yields short keys.
instance Interval B.ByteString where
  between a b = if n < B.length b then B.take n b
                else a  -- can't use full b, since not (b < b)
    where
      n = 1 + (length $ takeWhile (uncurry equals) $ B.zip a b)


-- | Type aliases to shorten cache type.
type TreeBackend mc k v    = KV.KVBackend mc (Ref (Node k v)) (Node k v)

-- | Type aliases to shorten result types.
type TreeResult m mc k v a = BTreeM m (Cstm.Param mc (Ref (Node k v)) (Node k v)) k v a



-- | helpers
ifM :: Monad m => m Bool -> m a -> m a -> m a
ifM m t f | true <- m = t
          | otherwise = f

equals a b = a `compare` b == EQ


-- | Make a new tree parameter from order, root node and cache parameter. When
-- no root node is given, 'Ref' 0 will be used and a new tree initialised
-- here. This may overwrite an existing tree. Is used together with 'execTree'.
makeParam :: (MonadIO mIO, C.Cache m p (Ref a) (Node k v))
             => Int                               -- ^ Order of tree.
             -> Maybe (Ref (Node k1 v1))          -- ^ Optional root node.
             -> p                                 -- ^ Cache parameter.
             -> mIO (Param p k1 v1)               -- ^ The result in monadIO
makeParam ord mroot cacheP = do
  -- Init tree when root is Nothing
  un <- liftIO $ newTVarIO []
  tv <- liftIO $ newTVarIO $ fromMaybe 0 mroot
  when (isNothing mroot) $
    liftIO $ C.eval cacheP $ C.store Nothing (Ref 0) emptyTree
  -- New channel for rebalance msgs
  rebCh <- liftIO $ newTChanIO
  return $ Param ord tv cacheP rebCh un
  where
    emptyTree = Leaf M.empty



-- | 'execTree' takes a tree parameter and a group of operations in a BTreeM
-- monad and exectures the operations.
execTree :: Param st k v                          -- ^ Tree parameter (see 'makeParam')
         -> BTreeM m st k v a                     -- ^ Tree instance
         -> m a                                   -- ^ The result inside the chosen monad
execTree p m =
  runReaderT (runBTreeM m) p



-- | Get next unique node reference id
newRef p = do
  ls <- Cstm.liftSTM $ readTVar $ unused p
  case ls of
    (r:rs) -> do Cstm.liftSTM $ writeTVar (unused p) rs
                 return r
    _      -> Cstm.fail newNode
  where
    newNode = do
      refids <- map Ref `fmap` replicateM 8 randomIO
      -- Collisions are rare, but not impossible
      forM_ refids $ \ref -> C.eval (state p) $ do
        n <- C.fetch ref
        case n of
          Nothing -> do Cstm.liftSTM $ do ls <- readTVar $ unused p
                                          writeTVar (unused p) $ ref : ls
          Just _  -> return ()


-- | Queue marked node for rebalancing
addMarked p k = do
  Cstm.liftSTM $ writeTChan (marked p) k

-- | Get root reference id
root' p = do
  liftIO $ atomically $ readTVar $ root p

-- | Fetch node from reference id; error when not found
fetch' ref = do
  n <- C.fetch ref
  return $ fromMaybe (error $ "invalid tree: " ++ show ref) n

-- | Store node in new ref
storeNew p parent ns = do
  refs <- replicateM (length ns) $ newRef p
  mapM_ (\(r, n) -> C.store (Just parent) r n) $ zip refs ns
  return refs

-- | Split a node in two, resulting in a new parent node
split p parent r (Leaf ks) = do
  [refL, refR] <- storeNew p r [leafL, leafR]
  when (keyL > keyB || keyR <= keyB) $ error "Between has failed!"
  let branch = Branch [keyB] [refL, refR]
  C.store (Just parent) r $! branch
  return branch
  where
    (keyL, keyR) = ((fst $ last keysL), (fst $ head keysR))
    keyB = keyL `between` keyR
    keys = M.toList ks
    (keysL, keysR) = L.splitAt (order p) keys
    leafL = Leaf $! M.fromAscList keysL
    leafR = Leaf $! M.fromAscList keysR

split p parent r (Branch keys rs) =
  do [refL, refR] <- storeNew p r [ Branch keysL rsL
                                  , Branch keysR rsR ]
     let branch = Branch [b] [refL, refR]
     mapM_ (Cstm.updateTag $ Just r) rs
     C.store (Just parent) r $! branch
     return branch
  where
    i = order p
    (keysL, (b : keysR)) = L.splitAt i keys
    (rsL, rsR) = L.splitAt (i+1) rs



-- | /O(log n)/. Insert key-value pair into current tree. After this operation
-- 'lookup' @k@ will yield @Just v@. If the key already exists it is overridden.
-- If you want the overridden value, or want different behaviour when the key
-- exists see 'modify'.
--
-- 'insert' may leave the tree /unbalanced/, skewed or with underfull nodes.
-- The tree can be re-balanced by starting a 'rebalanceProcess'.
--
-- > execTree p $ insert 42 "foobar"
insert :: ( MonadIO m, TreeBackend mc k v
          , Key k, Value v)
          => k                                    -- ^ key
          -> v                                    -- ^ value to associate with the key
          -> TreeResult m mc k v ()
insert k v = do _ <- modify const k v
                return ()


-- | /O(log n)/. Delete a key from the tree. The deleted value is returned as
-- @Just v@ if present, otherwise @Nothing@ is returned.
--
-- > execTree p $ delete 42
delete :: ( MonadIO m, TreeBackend mc k v
          , Key k, Value v)
          => k                                    -- ^ key
          -> TreeResult m mc k v (Maybe v)        -- ^ The previous value if present
delete k = modifyLeaf (findChild k) $ \p parent r ks -> do
  let (vOld, ks') = M.updateLookupWithKey (\_ _ -> Nothing) k ks
  C.store (Just parent) r $! Leaf ks'
  return vOld



-- | /O(log n)/. Lookup key in current tree.
--
-- > execTree p $ do insert 42 "foo"      -- ()
-- >                 a <- lookup 42       -- Just "foo"
-- >                 insert 42 "bar"      -- ()
-- >                 b <- lookup 42       -- Just "bar"
-- >                 delete 42            -- Just "bar"
-- >                 c <- lookup 42       -- Nothing
-- >                 return (a, b, c)     -- (Just "foo", Just "bar", Nothing)
lookup :: ( MonadIO m, TreeBackend mc k v
          , Key k, Value v)
          => k                                    -- ^ key
          -> TreeResult m mc k v (Maybe v)        -- ^ The value if present
lookup k = modifyLeaf (findChild k) $ \_ _ _ -> return . M.lookup k



-- | /O(log n)/. Replace the value of @k@ with @f v v'@, where @v'@ is the
-- current value. The old value @v'@ is returned after the replacement. If no
-- current value exist, @v@ is inserted.
--
-- The semantics is the same as @'Data.Map.insertLookupWithKey' . const@.
--
-- > execTree p $ do delete 42
-- >                 modify subtract 42 1    -- inserts    (42,  1)
-- >                 modify subtract 42 1    -- updates to (42,  0)
-- >                 modify subtract 42 1    -- updates to (42, -1)
modify :: ( MonadIO m, TreeBackend mc k v
          , Key k, Value v)
          => (v -> v -> v)                  -- ^ @f@ computes the new value from old and default.
          -> k                              -- ^ key
          -> v                              -- ^ Default value is used when no other is present
          -> TreeResult m mc k v (Maybe v)  -- ^ The previous value if present
modify f k v = do
  modifyLeaf (findChild k) $ \p parent r ks -> do
  let (vOld, ks') = M.insertLookupWithKey (const f) k v ks
      lf' = Leaf ks'
  if M.size ks' <= 2 * order p
    then do unless (vOld /= Nothing && fromJust vOld == ks' M.! k) $
              C.store (Just parent) r $! lf'
    else do Branch [bk] _ <- split p parent r lf'
            addMarked p bk
  return vOld
{-# INLINE modify #-}


-- modifyFromChannel ch = do
--   -- get first set of f k v
--   task@(_, _, k, _) <- liftIO $ atomically $ readTChan ch
--   -- lookup parent and child, execute task
--   (r, ks) <- modifyLeaf (findChild k) $ \p r ks -> do insert' p task r ks
--                                                       return (r, ks)
--   -- try to execute next task, too
--   task'@(_, _, k, _) <- liftIO $ atomically $ readTChan ch
--   next ch task' r
--   where
--     next ch task r = do
--       suc <- go task r
--       case suc of
--         True -> do
--           task <- liftIO $ atomically $ readTChan ch
--           next ch task r
--         False -> do
--           liftIO $ atomically $ writeTChan ch task
--           modifyFromChannel ch

--     go task@(_, _, k, _) r = do
--       p <- ask
--       liftIO $ C.eval (state p) $ do
--         n <- C.fetch r
--         case n of
--           (Just (Leaf ks))
--             -- Inside correct leaf
--             | M.size ks > 0 && (fst $ M.findMin ks) <= k && k <= (fst $ M.findMax ks) ->
--               do insert' p task r ks
--                  return True

--             | otherwise -> return False
--           _ -> return False

--     insert' p (tm, f, k, v) r ks = do
--       let (vOld, ks') = M.insertLookupWithKey (const f) k v ks
--           lf' = Leaf ks'
--       if M.size ks' <= 2 * order p
--         then do C.store r r lf'
--         else do b@(Branch [bk] _) <- split p lf'
--                 C.store r r b
--                 addMarked p bk
--       Cstm.liftSTM $ putTMVar tm vOld


findChild !k (Branch ks rs) =
  let idx = L.length $ L.takeWhile (<k) ks in rs !! idx


-- | Calculate the height of the tree, i.e. the longest node path from root to leaf.
height :: (MonadIO m, C.Cache m1 p (Ref (Node k v)) (Node k v)) => BTreeM m p k v Int
height = do p <- ask
            r <- root' p
            liftIO $ go p r
  where
    go p r = do
      n <- C.eval (state p) $ C.fetch r
      case n of
        Nothing -> error $ "height: invalid tree: " ++ show r
        Just (Leaf     ks) -> return 1
        Just (Branch _ rs) -> do h <- foldM (\h r -> do h' <- go p r
                                                        return $ max h h') 0 rs
                                 return $ 1 + h


-- | Modify a leaf in current tree and return result of modification
modifyLeaf :: (Ord k, MonadIO m, C.Cache mc p (Ref (Node k v)) (Node k v)) =>
              (Node k v -> Ref (Node k v)) ->
              (Param p k v -> Ref (Node k v) -> Ref (Node k v) -> M.Map k v -> mc a) ->
              BTreeM m p k v a
modifyLeaf pick f = retry 100 $ do p <- ask
                                   r <- root' p
                                   liftIO $ try $ go p r r
  where
    retry 0 m = do x <- m
                   either throw return x
    retry n m = do x <- m
                   case x of
                     Left  (_::ErrorCall) -> retry (n-1) m
                     Right a -> return a

    go p parent r = do
      res <- C.eval (state p) transaction
      either (go p r) return res
        where
          transaction =
            do n <- C.fetch r
               case n of
                 Just (Leaf ks) ->
                   do v <- f p parent r ks
                      return $! Right $! v
                 Just n -> return $! Left $! pick n
                 Nothing ->
                   error $ "modifyLeaf: invalid tree: " ++ show r
{-# INLINE modifyLeaf #-}


-- | A process for background rebalancing. Start inside its own thread, since
-- this will run forever. Stop by killing the thread.
--
-- > pid <- forkIO $ rebalanceProcess p
-- > -- Perform safe tree operations
-- > killThread pid
rebalanceProcess :: (MonadIO m, TreeBackend m2 k v, Key k, Value v)
                    => Param (CacheSTMP m2 k v) k v       -- ^ Tree parameter.
                    -> m (MVar ThreadId)                  -- ^ ThreadId
rebalanceProcess p = liftIO $ do
  mv  <- newEmptyMVar
  pid <- forkIO $ forever $ execTree p $ do
    p <- ask
    r <- root' p
    k <- liftIO $ atomically $ readTChan $ marked p
    x <- liftIO $ takeMVar mv
    rebalanceKey p r r k
    liftIO $ putMVar mv x
  putMVar mv pid
  return mv


-- | Rebalance until no more rebalancing can take place
rebalanceAll :: (MonadIO m, Key k, Value v,
                 C.Cache (CacheSTM m2 k v) (CacheSTMP m2 k v) (Ref (Node k v)) (Node k v),
                 KV.KVBackend m2 (Ref (Node k v)) (Node k v)) =>
                BTreeM m (CacheSTMP m2 k v) k v ()
rebalanceAll = ifM rebalance rebalanceAll $ return ()


-- | Perform rebalancing.
-- Moves the tree one step closer to rebalancing
rebalance :: (Key k, Value v, MonadIO m,
              C.Cache (CacheSTM m2 k v) (CacheSTMP m2 k v) (Ref (Node k v)) (Node k v),
              KV.KVBackend m2 (Ref (Node k v)) (Node k v)) =>
             BTreeM m (CacheSTMP m2 k v) k v Bool
rebalance = do p <- ask
               r <- root' p
               let ch = marked p
               mk <- liftIO $ atomically $
                     ifM (isEmptyTChan ch)
                     (return Nothing)
                     (Just `fmap` readTChan ch)
               case mk of
                 Just k  -> do rebalanceKey p r r k
                               return True
                 Nothing -> return False


rebalance' :: (Key k, Value v, MonadIO m,
              C.Cache (CacheSTM m2 k v) (CacheSTMP m2 k v) (Ref (Node k v)) (Node k v),
              KV.KVBackend m2 (Ref (Node k v)) (Node k v)) =>
             BTreeM m (CacheSTMP m2 k v) k v ()
rebalance' = do p <- ask
                r <- root' p
                let ch = marked p
                k <- liftIO $ atomically $ readTChan ch
                rebalanceKey p r r k

rebalanceKey p parent r k = do
  ord <- asks order
  mr  <- liftIO $ C.eval (state p) $ trans ord
  case mr of
    Nothing -> return ()
    Just r' -> rebalanceKey p r r' k
  where
    trans ord = do
      mn <- C.fetch r
      case mn of
        Nothing                 -> error $ "rebalanceKey: invalid tree: " ++ show r
        Just (Leaf _)           -> return Nothing
        Just (n@(Branch ks rs)) -> do
          let !cr = findChild k n
          mcn <- C.fetch cr
          case mcn of
            Nothing       -> error $ "rebalanceKey: invalid tree: cr = " ++ show cr
            Just (Leaf _) -> return Nothing  -- TODO: Handle empty leaf
            Just (cn@(Branch [k'] [r0, r1])) -> do
              let ks'        = L.insert k' ks
                  Just idx   = L.elemIndex k' ks'
                  (a, _ : b) = L.splitAt idx rs
                  rs'        = a ++ (r0 : r1 : b)
                  branch     = Branch ks' rs'
              if length rs' <= 2 * ord
                then do C.store (Just parent) r branch
                        C.remove Nothing cr
                        -- TODO: why can't we remove cr?
                else do branch'@(Branch _ frs) <- split p parent r branch
                        C.store (Just parent) r branch'
                        C.remove Nothing cr
                        addMarked p k'
              if k `equals` k' then return Nothing
                else return $ Just r
            Just (Branch cks crs) -> return $ Just cr



deleteByFst k = L.deleteBy cmpfst (k, undefined)
  where
    cmpfst (a, _) (b, _) = a `equals` b


size :: (Ord k, MonadIO m, C.Cache mc p (Ref (Node k v)) (Node k v)) =>
        BTreeM m p k v Int
size = do p <- ask
          r <- root' p
          go p r
  where
    go p r = do
      n <- liftIO $ C.eval (state p) $ fetch' r
      case n of
        Leaf ks -> return $ M.size ks
        Branch ks rs -> do
          tot <- foldM (\n r ->
                         do m <- go p r
                            return $ n + m
                       ) 0 rs
          return tot


-- | Convert the tree into a list of key-value pairs. This function may crash
-- if used together with 'rebalanceProcess'.
toList :: ( MonadIO m, TreeBackend mc k v, Key k, Value v)
          => TreeResult m mc k v [(k, v)]         -- ^ The list of (key, value) pairs.
toList = do p <- ask
            r <- root' p
            Cstm.withGeneration (state p) $ \n -> go n p r
  where
    go gen p r = do
      n <- liftIO $ C.eval (state p) $ Cstm.fetchGen gen r
      case n of
        Nothing             -> error $ "toList: invalid tree: " ++ show r
        Just (Leaf ks)      -> return $! M.toList ks
        Just (Branch ks rs) -> do
          ls <- mapM (go gen p) rs
          return $ concat ls


-- | Save the tree by flushing the underlying cache to the permanent store and
-- return a ref to the root node.
save :: ( MonadIO m, TreeBackend mc k v, Key k, Value v)
        => TreeResult m mc k v (Ref (Node k v))   -- ^ 'Ref' to the root node.
save = do
  p <- ask
  liftIO $ C.sync $ state p
  root' p


-- | Lookup minimum key
findMin :: (Ord k, MonadIO m, C.Cache mc p (Ref (Node k v)) (Node k v)) =>
           BTreeM m p k v (k, v)
findMin = modifyLeaf (\(Branch _ rs) -> head rs) $ \_ _ _ -> return . M.findMin


-- | Lookup maximum key
findMax :: (Ord k, MonadIO m, C.Cache mc p (Ref (Node k v)) (Node k v)) =>
           BTreeM m p k v (k, v)
findMax = modifyLeaf (\(Branch _ rs) -> last rs) $ \_ _ _ -> return . M.findMax



-- | Fold with key in left to right order.
foldli :: (MonadIO m, TreeBackend mc k v, Key k, Value v) =>
          (a -> k -> v -> a) -> a -> TreeResult m mc k v a
foldli f a = do p <- ask
                r <- root' p
                go p a r
  where
    go p a r = do
      n <- liftIO $ C.eval (state p) $ fetch' r
      case n of
        Leaf   ks    -> return $! M.foldlWithKey f a ks
        Branch ks rs -> foldM (go p) a rs


-- | Fold with key in right to left order.
foldri :: (MonadIO m, TreeBackend mc k v, Key k, Value v) =>
          (k -> v -> a -> a) -> a -> TreeResult m mc k v a
foldri f a = do p <- ask
                r <- root' p
                go p a r
  where
    go p a r = do
      n <- liftIO $ C.eval (state p) $ fetch' r
      case n of
        Leaf ks      -> return $! M.foldrWithKey f a ks
        Branch ks rs -> foldM (go p) a $ reverse rs




-- | A generalised way of querying the tree. Given two keys @a <= b@ the
-- function needs to answer @True@ or @False@ as to whether the interval @[a,
-- b]@ contains interesting keys. No all keys in the interval need be
-- interesting. This function will then return all interesting keys in an
-- efficient way.
search :: (MonadIO m, TreeBackend mc k v, Key k, Value v)
          => ((k, k) -> Bool)                     -- ^ @f@ defines interesting intervals
          -> TreeResult m mc k v [(k, v)]         -- ^ A list of chosen (key, value) pairs
search f = do p  <- ask
              r  <- root' p
              lb <- fst `fmap'` findMin -- lower bound
              ub <- fst `fmap'` findMax -- upper bound
              go p r lb ub
  where
    fmap' f m = m >>= return . f

    go p r lb ub  = do
      n <- liftIO $ C.eval (state p) $ fetch' r
      case n of
        Leaf   ks -> return $! filter (\(k, _) -> f (k, k)) $ M.toList ks
        Branch ks rs -> do
          let ints = findInts 0 (lb:ks ++ [ub])
          ls <- mapM (\(n, lb, ub) -> go p (rs!!n) lb ub) ints
          return $ concat ls
      where
        findInts (!n) (lb:ub:xs) =
          if f (lb, ub) then (n, lb, ub) : rest else rest
          where
            rest = findInts (n+1) (ub:xs)
        findInts _ _ = []


listNodes :: (MonadIO m, TreeBackend mc k v, Key k, Value v)
          => TreeResult m mc k v [Ref (Node k v)]         -- ^ A list of node names
listNodes = do p <- ask
               r <- root' p
               go p [r] [] []
  where
    go p []     [] res = return $! reverse res
    go p []     ys res = go p (concat $ reverse ys) [] res
    go p (x:xs) ys res = do
      n <- liftIO $ C.eval (state p) $ fetch' x
      case n of
        Leaf   ks    -> go p xs ys $! x : res
        Branch ks rs -> go p xs (reverse rs : ys) $! x : res


-- listNodes alternative that lists the nodes breadth-first:
--
-- listNodes :: (MonadIO m, TreeBackend mc k v, Key k, Value v)
--           => TreeResult m mc k v [Ref (Node k v)]         -- ^ A list of node names
-- listNodes = do p <- ask
--                r <- root' p
--                go p r
--   where
--     go p r = do
--       n <- liftIO $ C.eval (state p) $ fetch' r
--       case n of
--         Leaf   ks    -> return [r]
--         Branch ks rs -> do rest <- mapM (go p) rs
--                            return $! r : concat rest
