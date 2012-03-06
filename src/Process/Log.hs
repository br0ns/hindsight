module Process.Log
       ( log, trace, info, warning, critical, abort
       , Priority(..)
       , setupLogger
       )
       where

import Prelude hiding (log)

import System.IO (stderr)
import Control.Monad.Trans

import System.Log.Logger

import System.Log.Handler        (setFormatter)
import System.Log.Handler.Simple (streamHandler)
import System.Log.Formatter      (simpleLogFormatter)


log :: MonadIO m => Priority -> String -> String -> m ()
log prio cat msg =
  liftIO $ logM cat prio msg


trace :: MonadIO m => String -> String -> m ()
trace = log DEBUG


info :: MonadIO m => String -> String -> m ()
info = log INFO


warning :: MonadIO m => String -> String -> m ()
warning = log WARNING


critical :: MonadIO m => String -> String -> m ()
critical = log CRITICAL


abort :: MonadIO m => String -> String -> m a
abort name msg = do critical name msg
                    error msg


setupLogger :: Priority -> IO ()
setupLogger prio =
  do streamH <- streamHandler stderr prio
     let handlers = map (flip setFormatter $ format) [streamH]
     updateGlobalLogger rootLoggerName $
       (setLevel DEBUG . setHandlers handlers)
  where
    format = simpleLogFormatter "$time, $loggername [$prio]: $msg"