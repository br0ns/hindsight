{-# LANGUAGE BangPatterns #-}

module Channel
       ( WriteChannel
       , ReadChannel
       , newChannel
       , newChannelP
       , newUnboundedChannel
       , newUnboundedChannelP
       , send
       , sendP
       -- , trySend
       -- , trySendP
       , canSend
       , canSendP
       , receive
       , receiveP
       , oneOf
       , dupC
       , dupCP
       -- , tryReceive
       -- , tryReceiveP
       -- , unReceive
       -- , unReceiveP
       , size
       , sizeP
       , stress
       , stressP
       -- , isEmpty
       -- , isEmptyP
       ) where

import Control.Concurrent.STM
import Control.Monad.Trans
import Control.Monad

data Chan a =
  Chan  { maxSize   :: !(Maybe Int)
        , channel   :: TChan a
        , readSize  :: TVar Int
        , writeSize :: TVar Int
        }

newtype WriteChannel a = WChan { getWChan :: Chan a }
newtype ReadChannel  a = RChan { getRChan :: Chan a }

type ChanPair a = (WriteChannel a, ReadChannel a)

dupC :: ReadChannel a -> STM (ReadChannel a)
dupC (RChan ch @ (Chan _ c _ _)) = do
  c' <- dupTChan c
  return $ RChan $ ch { channel = c' }

dupCP :: MonadIO m => ReadChannel a -> m (ReadChannel a)
dupCP = liftIO . atomically . dupC

newCP :: MonadIO m => Maybe Int -> m (ChanPair a)
newCP m = do
    c <- liftIO $ newTChanIO
    rs <- liftIO $ newTVarIO 0
    ws <- liftIO $ newTVarIO 0
    let ch = Chan m c rs ws
    return (WChan ch, RChan ch)

newC :: Maybe Int -> STM (ChanPair a)
newC m = do
    c <- newTChan
    rs <- newTVar 0
    ws <- newTVar 0
    let ch = Chan m c rs ws
    return (WChan ch, RChan ch)

newChannelP :: MonadIO m => Int -> m (ChanPair a)
newChannelP m = newCP $ Just m

newChannel :: Int -> STM (ChanPair a)
newChannel m = newC $ Just m

newUnboundedChannelP :: MonadIO m => m (ChanPair a)
newUnboundedChannelP = newCP Nothing

newUnboundedChannel :: STM (ChanPair a)
newUnboundedChannel = newC Nothing

canSend :: WriteChannel a -> STM Bool
canSend (WChan (Chan (Just m) _ _ wrTV)) = do
  sz <- readTVar wrTV
  return $ sz > m
canSend _ = return True

canSendP :: MonadIO m => WriteChannel a -> m Bool
canSendP = liftIO . atomically . canSend

(?<) :: Maybe Int -> Int -> Bool
Just x ?< y = x < y
Nothing ?< _ = False

send :: WriteChannel a -> a -> STM ()
send (ch@(WChan (Chan m c _ wrTV))) x =
  do sz <- size ch
     when (m ?< sz)
       retry
     wsz <- readTVar wrTV
     writeTVar wrTV $! wsz + 1
     writeTChan c $! x


sendP :: MonadIO m => WriteChannel a -> a -> m ()
sendP ch = liftIO . atomically . send ch

receive :: ReadChannel a -> STM a
receive (RChan (Chan _ c rdTV _)) = do
  x <- readTChan c
  rsz <- readTVar rdTV
  writeTVar rdTV $ rsz + 1
  return x

receiveP :: MonadIO m => ReadChannel a -> m a
receiveP = liftIO . atomically . receive

oneOf :: ReadChannel a -> ReadChannel b -> STM (Either a b)
oneOf chA chB =
  (fmap Left  $ receive chA) `orElse`
  (fmap Right $ receive chB) `orElse`
  (retry)

class Sizeable c where
  size   :: c a -> STM Int
  stress :: c a -> STM Double

sizeP :: (MonadIO m, Sizeable c) => c a -> m Int
sizeP = liftIO . atomically . size

stressP :: (MonadIO m, Sizeable c) => c a -> m Double
stressP = liftIO . atomically . stress

instance Sizeable WriteChannel where
  size   (WChan ch) = size' ch
  stress (WChan ch) = stress' ch

instance Sizeable ReadChannel where
  size   (RChan ch) = size' ch
  stress (RChan ch) = stress' ch

size' :: Chan a -> STM Int
size' (Chan _ _ rdTV wrTV) = do
  r <- readTVar rdTV
  w <- readTVar wrTV
  return $ w - r

stress' :: Chan a -> STM Double
stress' (Chan Nothing _ _ _) = return 0
stress' ch@(Chan (Just m) _ _ _) = do
  s <- size' ch
  return $ fromIntegral s / fromIntegral m
