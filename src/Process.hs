{-# LANGUAGE MultiParamTypeClasses
           , FlexibleInstances
           , GeneralizedNewtypeDeriving
           , DeriveFunctor
           , DeriveDataTypeable
           , ExistentialQuantification
           , UndecidableInstances
           , ScopedTypeVariables
  #-}

module Process
       ( Process
       , ConfigP (..)
       , Channel
       , Supervisor
       , defaultConfigP
       , spawn
       , with
       , newM
       , new
       , (-|-)
       , (-|<)
       , combineB
       , replicateB
       , parallel
       -- , parallel2
       , Reply
       , send
       , receive
       , sendReply
       , sendBlock
       , flushChannel
       , replyTo
       , log
       , debug
       , info
       , warning
       , critical
       , abort
       , setupLogger
       , Priority(..)
       )
       where

import Prelude hiding (log, catch)
import Control.Exception

import Control.Concurrent (forkIO, myThreadId, threadDelay)
import Control.Concurrent.STM

import Control.Monad.State.Strict
import Control.Monad.Reader
import Control.Monad.Identity

import Supervisor
import qualified Channel as C
import Channel hiding (send, receive)

import qualified Process.Log as L
import Process.Log (setupLogger, Priority (..))

import Data.List
import Data.Typeable

data Status
  = Busy
  | Idle
  | Dead

data ConfigP =
  ConfigP { name        :: String
          , channelSize :: Int
          }

type Reply a = TMVar a
type Semaphore = Reply ()

type Channel msg = C.WriteChannel (Packet msg)
-- type Supervisor = Maybe (WriteChannel CliMessage)
type Supervisor = Maybe (ReadChannel SupMessage)

data Packet msg
  = Flush Semaphore
  | Blocking msg Semaphore
  | NonBlocking msg

defaultConfigP = ConfigP { name = "<no name given>"
                         , channelSize = 2
                         }

newtype Process msg =
  Process { runP :: Supervisor -> IO (Channel msg) }

spawn = flip runP

newtype ProcessM' m a =
  -- ProcessM { runProcessM :: ReaderT ConfigP (StateT Status m) a }
  ProcessM { runProcessM :: ReaderT ConfigP  m a }
  deriving ( Monad
           , MonadIO
           , Functor
           , MonadReader ConfigP
           , MonadState s
           , MonadTrans)

type ProcessM m = ProcessM' m ()


data StopException = StopException
  deriving (Show, Eq, Typeable)

instance Exception StopException

with :: Process msg -> (Channel msg -> IO a) -> IO a
with p k = do
  (wsup, rsup) <- newUnboundedChannelP
  ch <- spawn (Just rsup) p
  a <- k ch
  flushChannel ch
  C.sendP wsup Stop
  return a

newM :: MonadIO m =>
        ( SupMessage -> ProcessM m
        , msg        -> ProcessM m
        ,               ProcessM m
        ,               ProcessM m
        , m ()       -> IO ()
        ) -> ConfigP -> Process msg
newM (hSup', hMsg', hInit, hFlush, runner') cfg = Process $ \msup ->
  do (wCh, rCh) <- newChannelP $ channelSize cfg
     forkIO $ runner
       $ flip runReaderT cfg
       $      runProcessM $ do hInit
                               go rCh msup
     return wCh
    where
      runner m = runner' m `catches`
                 [ Handler (\(e :: StopException) -> L.info (name cfg) "Shutting down")
                 , Handler (\(e :: SomeException) -> L.critical (name cfg) $ show e)]
      go rCh msup =
        case msup of
          Just rSup -> do
            -- (wCli, rSup) <- newUnboundedChannelP
            -- register wSup wCli
            rSup' <- dupCP rSup
            goSup rSup'
          Nothing -> goNoSup
          where
            goSup rSup = forever $
              do msg <- liftIO $ atomically $ rSup `oneOf` rCh
                 either hSup hMsg msg
            goNoSup = forever $
              do msg <- receive rCh
                 hMsg msg
      hSup msg = do
        hSup' msg
        when (msg == Stop) $ do
          throw StopException
      hMsg msg =
        case msg of
          Flush sem -> do
            hFlush
            release sem
          Blocking msg sem -> do
            hMsg' msg
            release sem
          NonBlocking msg ->
            hMsg' msg
      release = flip replyTo ()
      register wSup wCli = do
        pid <- threadId
        name <- asks name
        C.sendP wSup $ Supervisor.Register wCli pid name

new :: ( SupMessage -> ProcessM IO
       , msg        -> ProcessM IO
       ,               ProcessM IO
       ,               ProcessM IO
       ) -> ConfigP -> Process msg
new (hSup, hMsg, hInit, hFlush) = newM (hSup, hMsg, hInit, hFlush, id)

infixr 3 -|-
(-|-) :: (Channel msgA -> Process msg) ->
         Process msgA ->
         Process msg
fp -|- p = Process $ \msup ->
  do ich <- spawn msup p
     spawn msup $ fp ich

infixr 3 -|<
(-|<) :: (Channel msgA -> Channel msgB -> Process msg) ->
         Process msgA -> Process msgB -> Process msg
(fp -|< p0) p1 = Process $ \msup ->
  do chA <- spawn msup p0
     chB <- spawn msup p1
     spawn msup $ fp chA chB

combineB :: [Process msg] -> Process msg
combineB ps = Process $ \msup ->
  do chs <- mapM (spawn msup) ps
     (wCh, rCh) <- newChannelP 10
     forkIO $ flip evalStateT chs $ go msup rCh
     return wCh
     where
       go msup rCh =
         case msup of
           Just rSup -> do
             rSup' <- dupCP rSup
             goSup rSup'
           Nothing -> goNoSup
           where
             goSup rSup = do
               msg <- liftIO $ atomically $ rSup `oneOf` rCh
               case msg of
                 Left Stop -> return ()
                 Left _ -> goSup rSup
                 Right m -> do
                   hMsg m
                   goSup rSup
             goNoSup = forever $ do
               msg <- receive rCh
               hMsg msg
       hMsg msg = do
         chs <- get
         ss <- mapM stressP chs
         let ch = fst $ minimumBy (\(chA, sA) (chB, sB) -> compare sA sB)
                  $ zip chs ss
         case msg of
           Flush sem -> do
             mapM_ flushChannel chs
             release sem
           pkt ->
             C.sendP ch pkt
       release = flip replyTo ()

replicateB num = combineB . replicate num

parallel :: Int -> (Channel wMsg -> Process rMsg) ->
                    Channel wMsg -> Process rMsg
parallel num p ch = replicateB num $ p ch

-- parallel2 :: Int -> (WriteChannel wMsgA -> WriteChannel wMsgB -> Process rMsg) ->
--                      WriteChannel wMsgA -> WriteChannel wMsgB -> Process rMsg
-- parallel2 num p chA chB = replicateRR num $ p chA chB

send :: MonadIO m => Channel msg -> msg -> m ()
send ch msg = liftIO . atomically $ C.send ch (NonBlocking msg)

sendReply :: MonadIO m => Channel msg -> (Reply a -> msg) -> m a
sendReply ch msgf =
  sendRep ch (NonBlocking . msgf)

sendBlock :: MonadIO m => Channel msg -> msg -> m ()
sendBlock ch msg =
  sendRep ch $ Blocking msg

flushChannel :: MonadIO m => Channel msg -> m ()
flushChannel ch =
  sendRep ch Flush

sendRep ch msgf = liftIO $ do
  v <- atomically newEmptyTMVar
  C.sendP ch $ msgf v
  atomically $ readTMVar v

receive ch = liftIO . atomically $ C.receive ch

replyTo :: MonadIO m => Reply a -> a -> m ()
replyTo v a = liftIO $ atomically $ putTMVar v $! a

{-| Logging inside the ProcessM monad -}
log :: MonadIO m => L.Priority -> String -> ProcessM m
log prio msg =
  do name <- asks name
     tid  <- threadId
     L.log prio (name ++ "-" ++ tid) msg

threadId :: MonadIO m => m String
threadId = do id <- liftIO myThreadId
              return $ last $ words $ show id


debug, info, warning, critical, abort
 :: MonadIO m => String -> ProcessM m
debug     = log L.DEBUG
info      = log L.INFO
warning   = log L.WARNING
critical  = log L.CRITICAL
abort msg = do critical msg
               error msg
