{-# LANGUAGE ScopedTypeVariables #-}

module Process.External
       ( Message (..)
       , external
       )
       where

import Util

import Prelude hiding (catch)

import Crypto.Simple (encrypt, decrypt, readMasterKey)

import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.ByteString.Char8 (ByteString)

import System.FilePath
import System.Directory
import Control.Monad.Trans
import Data.Functor
import Data.Maybe

import Control.Concurrent (threadDelay)
import Control.Exception
import Control.Monad

import Config
import Process
import Supervisor
import qualified Process.Stats as Stats

import qualified Codec.Compression.Snappy as Snappy

import System.Process
import System.IO
import System.Exit
import System.Time

data Message
  = PutCallback !(IO ()) !ByteString !ByteString
  | Put !ByteString !ByteString
  | Get !ByteString !(Reply ByteString)
  | Del !ByteString

external statCh masterKey mbbufdir modDir = new (handleSup, handleMsg, info "Started", return ())
  where
    handleSup msg =
      case msg of
           Stop -> info "Nothing to do here. Go ahead and stop me."
           _    -> warning $ "Ignoring supervisor message: " ++ show msg

    handleMsg msg = {-# SCC "External" #-}
      case msg of
        PutCallback callback k v -> liftIO $ do
          runPut k $ comp v
          callback
          completed v

        Put k v -> do
          liftIO $! runPut k $! comp v
          completed v

        Get k rep ->
          case mbbufdir of
            Just dir -> do
              let bufFile = dir </> byteStringToFileName k
              ex <- liftIO $ try $ B.readFile bufFile
              case ex of
                Right buf -> replyTo rep buf
                Left (e :: SomeException) -> do
                  do blob <- getBlob
                     liftIO $ B.writeFile bufFile blob
                     replyTo rep blob
            Nothing -> getBlob >>= replyTo rep
          where
            getBlob = do
              blob <- liftIO $! fmap decomp $! runGet k
              completed blob
              return blob

        Del k ->
          liftIO $ runDel k

    completed x = do
      now <- liftIO getClockTime
      send statCh $ Stats.Completed now $ fromIntegral $ B.length x

    comp   = case compressionMode of
                  Snappy         -> Snappy.compress
                  CompressionOff -> id

    decomp = case compressionMode of
                  Snappy         -> Snappy.decompress
                  CompressionOff -> id

    proc cmd args input = do
      (hIn, hOut, hErr, hProc) <- runInteractiveProcess (modDir </> cmd) args
                                  Nothing -- inherit workdir
                                  Nothing -- inherit environment
      -- Write input
      when (isJust input) $ do
        B.hPut hIn $ fromJust input
        hFlush hIn
      hClose hIn
      -- Wait for process
      -- Get output
      bytes <- B.hGetContents hOut
      hClose hOut
      -- Get exit code
      ecode <- waitForProcess hProc
      case ecode of
        ExitSuccess      -> hClose hErr
        ExitFailure code -> do err <- B.hGetContents hErr
                               error $ "Got ExitFailure: " ++ show code ++ ": "
                                 ++ show (modDir </> cmd, args, err)
      return bytes

    runGet k = do
      cipherT <- proc "get" [byteStringToFileName k] Nothing
      now <- liftIO getClockTime
      send statCh $ Stats.Transfered now $ fromIntegral $ B.length cipherT
      return $ either die id $ decrypt masterKey cipherT

    die s = error $ "External: Decrypt: " ++ s

    runPut k plainT = do
      cipherT <- encrypt masterKey plainT
      now <- liftIO getClockTime
      send statCh $ Stats.Transfered now $ fromIntegral $ B.length cipherT
      void $ proc "put" [byteStringToFileName k] $ Just cipherT

    runDel k =
      void $ proc "del" [byteStringToFileName k] Nothing
