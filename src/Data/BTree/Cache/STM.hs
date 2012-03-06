{-# LANGUAGE GeneralizedNewtypeDeriving
           , FlexibleContexts
           , FlexibleInstances
           , MultiParamTypeClasses
           , UndecidableInstances
           , RankNTypes
           , ScopedTypeVariables
  #-}

module Data.BTree.Cache.STM where

import Data.Hashable (Hashable)
import qualified Data.List  as L
import qualified Data.Map   as M
import qualified Data.Graph as G

import qualified Data.BTree.Cache.Class     as C
import qualified Data.BTree.HashTable.STM   as H
import qualified Data.Serialize             as S

import qualified Data.BTree.KVBackend.Class as KV

import Control.Concurrent.STM
import Control.Concurrent

import Control.Monad
import Control.Monad.Trans
import Control.Monad.Reader

import qualified Data.ByteString as B

import Data.Either
import Data.Maybe
import Data.Time.Clock
import Debug.Trace
import System.IO

import Control.Monad.Error


data State k v = Read             !(Maybe v)
               | Write !(Maybe k) !(Maybe v)
               deriving Eq

instance Show (State k v) where
  show (Read    a) = "Read:  " ++ mtoS a
  show (Write k a) = "Write: " ++ mtoS a

mtoS a = if isJust a then "Just" else "Nothing"

type AccessTime = UTCTime


data Exist = Exist | NoExist
           deriving (Eq)

data Ref k v = Ref {
  refST  :: TVar (Either (State k v)
                  (State k v, Int, State k v)),  -- Current state
  refExt :: TVar Exist                           -- Exists in external storage?
  }


data Param m k v = Param {
    cacheSize :: Int
  , table     :: H.HashTableSTM k (Ref k (Either B.ByteString v))
  , toIO      :: forall a. m a -> IO a
  , flushQ    :: TVar [(k, Ref k v)]
  , timestamp :: UTCTime
  , genId     :: TVar Int -- Generation ID
  , genActive :: TVar Int -- Active users of maintained generation
  }

trace = id

newtype CacheSTM m k v a = CacheSTM { runCacheSTM :: ReaderT (Param m k v) (ErrorT (IO ()) STM) a }
                         deriving (Monad, MonadReader (Param m k v), MonadError (IO ()), Functor)

instance Error (IO ())


stm m = lift $ lift m


evalCacheSTM :: Param m k v -> CacheSTM m k v a -> IO a
evalCacheSTM p m = do
  mer <- atomically $ runErrorT $ runReaderT (runCacheSTM m) p
  case mer of
    Left  io -> io >> evalCacheSTM p m  -- perform IO and retry
    Right a  -> return a                -- return final result


sizedParam :: Int -> (forall a. m a -> IO a) -> IO (Param m k v)
sizedParam s f = do
  ts <- getCurrentTime
  ht <- H.newSized s
  mv <- atomically $ newTVar []
  gn <- atomically $ newTVar 0
  ga <- atomically $ newTVar 0
  return $ Param s ht f mv ts gn ga


getRef k = do
  p  <- ask
  ht <- asks table
  ev <- asks toIO
  mr <- stm $ H.lookup ht k
  case mr of
    Nothing -> fetchRef p ev ht
    Just r  -> return r
  where
    fetchRef p ev ht = throwError $ (ev $ KV.fetch k) >>= createRef p ht
    createRef p ht bytes = do
      atomically $ do
        l <- H.lookup ht k
        case l of
          Just _ -> return ()
          Nothing -> do
            tvst <- newTVar $ Left $ Read $ Left `fmap` bytes
            tvex <- newTVar Exist
            let ref = Ref tvst tvex
            H.insert ht k ref


newRef t k v = do
  ht <- asks table
  r@(Ref tv _) <- getRef k
  -- maybeQueue False tv (k, r)
  update tv $! Write t v
  where
    update tv x = do
      gt <- asks genId
      ga <- asks genActive
      stm $ do
        gid <- readTVar gt -- Generation ID
        act <- readTVar ga -- Active users
        s   <- readTVar tv -- State of ref
        case s of
          Left o | act == 0  -> writeTVar tv $! Left  $! x
                 | otherwise -> writeTVar tv $! Right $! (x, gid, o)
          Right (x', n, o)
            | act == 0  -> writeTVar tv $! Left  $! x
            | gid /= n  -> writeTVar tv $! Right $! (x, gid, x')
            | otherwise -> writeTVar tv $! Right $! (x, gid, o )


maybeQueue force t x =
  if force then update
  else do
    s <- stm $ readTVar t
    case s of
      Left  (Write _ _)       -> return () -- Already in queue
      Right (Write _ _, _, _) -> return ()
      _                       -> update
  where
    update = do
      qt <- asks flushQ
      stm $ do q <- readTVar qt
               writeTVar qt $! x : q


store t k v = CacheSTM $ newRef t k $! Just $! Right v


-- fetch :: (Eq k, NFData k, Hashable k, KV.KVBackend IO k v) => k -> CacheSTM m k v (Maybe v)
fetch k = CacheSTM $ do
  r@(Ref tv _) <- getRef k
  s <- stm $ readTVar tv
  case s of
    Left x          -> return $! value x
    Right (x, _, _) -> return $! value x

fetchGen n k = CacheSTM $ do
  r@(Ref tv _) <- getRef k
  s <- stm $ readTVar tv
  return $ value $ getGen n s


remove t k = CacheSTM $ newRef t k Nothing


updateTag t k = CacheSTM $ do
  ht <- asks table
  x  <- stm $ H.lookup ht k
  case x of
    Nothing           -> return ()
    Just (Ref tv _) -> do
      s <- stm $ readTVar tv
      case s of
        Left  (Write _ v)       -> stm $ writeTVar tv $! Left  $! Write t v
        Right (Write _ v, n, o) -> stm $ writeTVar tv $! Right $! (Write t v, n, o)
        _ -> return ()


keys = CacheSTM $ do
  ht <- asks table
  stm $ H.keys ht


debug  a = liftIO $ do print a
                       hFlush stdout

getGen n (Left s) = s
getGen n (Right (news, m, olds))
  | n == m    = olds
  | otherwise = news

flipWrite x (Left  (Write _ x'))       | x == x' = Left  $! (Read x)
flipWrite x (Right (Write _ x', n, o)) | x == x' = Right $! (Read x', n, o)
flipWrite x (Right (c, n, Write _ x')) | x == x' = Right $! (c, n, Read x')
flipWrite _ s = s


equals a b = value a == value b


value v = case v of
  Read    x -> either decode id `fmap` x
  Write _ x -> either decode id `fmap` x
  where
    decode = either error id . S.decode


withGeneration p f = do
  -- start generation
  n <- liftIO $ atomically $ do
    a <- readTVar $ genActive p
    n <- readTVar $ genId p
    writeTVar (genActive p) $! a + 1
    if a > 0 then return n
      else do writeTVar (genId p) $! n + 1
              return $! n + 1
  -- compute
  x <- f n

  -- end generation
  liftIO $ atomically $ do
    a <- readTVar $ genActive p
    writeTVar (genActive p) $! a - 1

  -- yield result
  return x


flush p = do
  nowSize <- atomically $ H.size ht
  gen     <- atomically $ readTVar $ genId p
  when (nowSize > maxSize) $ do
    flush =<< (atomically $ H.toList ht)
  where
    ht      = table  p
    maxSize = cacheSize p

    flush ks = do
      mapM_ (evalCacheSTM p . flushKey ht) ks


flushKey ht (k, r@(Ref tvst tvex)) = CacheSTM $ do
  tvga <- asks genActive
  tvgi <- asks genId
  stm $ do
    act <- readTVar tvga
    gen <- readTVar tvgi
    s   <- readTVar tvst
    case s of
      Left  (Read  _) ->
        H.delete ht k >> return Nothing

      Right (Read    s, n, _) | act == 0 || n /= gen ->
        H.delete ht k >> return Nothing

      Right (Write t s, n, _) | act == 0 || n /= gen ->
        (writeTVar tvst $! Left $! Write t s) >> return Nothing

      Left (Write t (Just (Right v))) ->
        (writeTVar tvst $! Left $! Write t $! Just $! Left $! S.encode v)
        >> return Nothing

      Right (Write t (Just (Right v)), n, o) ->
        (writeTVar tvst $! Right $! (Write t $! Just $! Left $! S.encode v, n, o))
        >> return Nothing

      _ -> return $ Just s


sync p = do
  withGeneration p $ \gen -> do
    -- sync
    -- TODO: use flush queue
    ks <- atomically $ H.toList $ table p

    ls <- forM ks $ \(k, r@(Ref tv _)) -> do
      s <- atomically $ readTVar tv
      case getGen gen s of
        Write (Just t) _ -> return $! Just $! Left  (t, k, r)
        Write Nothing  _ -> return $! Just $! Right (k, r)
        _                -> return $! Nothing

    let (lefts, rights) = partitionEithers $ catMaybes ls

    mapM_ (evalCacheSTM p . go gen) $ sortByTag lefts
    mapM_ (evalCacheSTM p . go gen) $ rights
  where
    sortByTag :: Ord k => [(k, k, Ref k v)] -> [(k, Ref k v)]
    sortByTag ls =
      let m         = M.fromList $ zip (map (\(_, k, _) -> k) ls) [0..]
          (g, f, _) = G.graphFromEdges
                      [((k, r), i, maybe [] return $ M.lookup t m)
                      | ((t, k, r), i) <- zip ls [0..]]
      in map (\(p, _, _) -> p) $ map f $ G.topSort g

    go gen (k, (r@(Ref tv tvex))) = CacheSTM $ do
      s  <- stm $ readTVar tv
      ex <- stm $ readTVar tvex
      case getGen gen s of
        -- TODO: handle exist
        Write _ Nothing | ex == Exist -> throwError write
                        | otherwise   -> stm $ update Nothing

        Write _ (Just v) -> throwError write
        _ -> return ()

      where
        write = do
          s <- atomically $ readTVar tv
          case getGen gen s of
            Write _ Nothing  -> (toIO p $ KV.remove k  ) >> (atomically $ update Nothing)
            Write _ (Just v) -> (toIO p $ KV.store  k $ either id S.encode $ v)
                                >> (atomically $ update (Just v))
            _              -> return ()

        update v = do
            s <- readTVar tv
            writeTVar tv $! flipWrite v s


liftSTM  = CacheSTM . stm
fail     = CacheSTM . throwError


instance ( Show k, S.Serialize k, S.Serialize v, Ord k, Eq k, Eq v
         , Hashable k, KV.KVBackend m k B.ByteString) =>
         C.Cache (CacheSTM m k v) (Param m k v) k v where
  store  = store
  fetch  = fetch
  remove = remove
  sync   = sync
  eval   = evalCacheSTM