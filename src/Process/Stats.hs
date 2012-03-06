{-# LANGUAGE ScopedTypeVariables #-}

module Process.Stats
       ( Message (..)
       , stats
       )
       where

import Prelude hiding (catch, lookup)

import System.Directory
import System.FilePath
import qualified System.IO as IO

import Util

import Channel
import Process
import Supervisor

import Control.Exception
import Control.Monad.ST
import Control.Monad.Reader
import Control.Concurrent
import Control.Concurrent.STM

import Data.Word

import qualified Data.BTree.Cache.STM as Cache
import qualified Data.BTree.Class as C
import qualified Data.BTree.BTree as T
import qualified Data.BTree.KVBackend.Files as Back
import Data.BTree.KVBackend.Class (KVBackend)
import Data.BTree.Types (Ref, Node)

import System.Time       (ClockTime, getClockTime)
import System.Time.Utils (clockTimeToEpoch)

import Text.Printf
import Term


type Clock = Int

data Stats = Stats {
    doShow        :: !Bool
  , doSay         :: !Bool
  , status        :: !String
  , finalGoal     :: !(Maybe Word64)
  , completed     :: !Word64
  , completedWin  :: ![(Clock, Word64)]
  , transfered    :: !Word64
  , transferedWin :: ![(Clock, Word64)]
  }


data Message
  = Say !String
  | SetMessage !String
  | SetGoal    !Word64
  | ClearGoal
  | Completed  !ClockTime !Word64
  | Transfered !ClockTime !Word64
  | DontSay
  | Quiet

stats delay windowSize = newM (hSup, hMsg, init, hFlush, run)
  where
    init = do
      info "Started"

    hSup m =
      case m of
        Stop -> do
          info "Hammertime"
          (mv, pid) <- lift ask
          liftIO $ withMVar mv $ const $ do
            killThread pid
            maxCols <- getTermCols
            putStr $ "\r" ++ replicate maxCols ' ' ++ "\r"

        _    -> warning $ "Ignoring superviser message: " ++ show m


    hMsg m =
      case m of
        Say        msg   -> do
          mv <- lift $ asks fst
          liftIO $ withMVar mv $ \s ->
            when (doSay s) $ do
              maxCols <- getTermCols
              putStrLn $ "\r" ++ msg ++ replicate (maxCols - length msg) ' '

        Quiet            -> do
          upd $ \s -> s { doShow = False }
          liftIO $ do
            maxCols <- getTermCols
            putStr $ "\r" ++ replicate maxCols ' ' ++ "\r"

        DontSay          -> upd $ \s -> s { doSay = False }

        SetMessage msg   -> upd $ \s -> s { status    = msg }
        SetGoal    goal  -> upd $ \s -> s { finalGoal = Just goal, completed = 0 }
        ClearGoal        -> upd $ \s -> s { finalGoal = Nothing }

        Completed  clk b -> upd $ \s -> s { completed    = completed s + b
                                          , completedWin = (clk, b) `append` completedWin s }

        Transfered clk b -> upd $ \s -> s { transfered    = transfered s + b
                                          , transferedWin = (clk, b) `append` transferedWin s}
      where
        append (clk, b) l = (clockTimeToEpoch clk, b) : l
        upd f = do mv <- lift $ asks fst
                   liftIO $ modifyMVar mv $ \s -> return (f s, ())

    hFlush = return ()

    statsProcess mv = forM_ (cycle "/-\\|") $ \spinner -> do
      threadDelay delay
      -- Update and get current status
      s <- modifyMVar mv $ \s -> do
        window <- subtract windowSize `fmap` clockTimeToEpoch `fmap` getClockTime
        let cwin = takeLimit window $ completedWin  s
            twin = takeLimit window $ transferedWin s
            s' = s { completedWin = cwin, transferedWin = twin }
        return (s', s')
      when (doShow s) $ do
        -- Format and print status
        let cTot = completed  s
            tTot = transfered s
            cAvg = avg $ map snd $ completedWin  s
            tAvg = avg $ map snd $ transferedWin s

            cSpeed = formatBytes (cAvg :: Double) ++ "/s"
            tSpeed = formatBytes (tAvg :: Double) ++ "/s"

            per = maybe [spinner] (percentStr $ 100 * cTot) $ finalGoal s
            msg = status s

            overhead = printf "\r> %s [%s => %s]: " per cSpeed tSpeed :: String

        -- printf can throw exceptions :/
        maxCols <- getTermCols
        putStr $ overhead ++ fixSize (max 0 (maxCols - length overhead)) msg
        IO.hFlush IO.stdout
        `catch` \(e :: SomeException) -> putStr "\r"

      where
        takeLimit w = filter (\(clk, _) -> w <= clk)
        avg l | length l == 0 = 0
              | otherwise     = (fromIntegral $ sum l) / (fromIntegral windowSize)

        percentStr p tot =
          printf "%.2f%%" $ (fromIntegral p / fromIntegral tot :: Double)

        fixSize n s
          | length s <= n = s ++ replicate (negate prefix) ' '
          | otherwise     = ".." ++ drop (prefix + 2) s
          where prefix = length s - n

        formatBytes n
          | n <= 2048        = printf "%.2f B" n
          | n <= 2048 * 1024 = printf "%.2f KB" $ n / 1024
          | otherwise        = printf "%.2f MB" $ n / 1024**2


    run m = do mv <- newMVar empty
               pid <- forkIO $ statsProcess mv
               void $ runReaderT m (mv, pid)

    empty = Stats True True "" Nothing 0 [] 0 []


