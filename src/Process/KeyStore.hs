{-# LANGUAGE BangPatterns
           , ScopedTypeVariables
  #-}

module Process.KeyStore
       ( Message (..)
       , Contents (..)
       , keyStore
       , recover
       )
       where

import Prelude hiding (catch)

import qualified Data.ByteString.Char8 as B
import Data.ByteString.Char8 (ByteString)

import Control.Exception

import System.IO hiding (hFlush)
import System.Directory
import System.Posix.Files (getFileStatus)

import Control.Monad.Trans
import Control.Monad.Trans.Resource hiding (try)
import Control.Monad

import Data.Maybe
import qualified Data.Set as Set

import Config (chunkMode, ChunkMode(..))
import Process
import Rollsum
import Supervisor

import qualified Process.Stats as Stats

import qualified Data.ByteString.Lazy as BL

-- import qualified Data.Enumerator.Binary as EB
-- import qualified Data.Enumerator.List as EL
-- import Data.Enumerator hiding (mapM, sequence)

import Data.Conduit hiding (Stop)
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB

import qualified Process.Index as Idx
import qualified Process.HashStore as HS

type Meta = ByteString
type Key = ByteString
type Version = ByteString

data Message
  = Insert !(Maybe Version) !Key (IO Meta) !Contents !(Reply (Either String Bool))
  | Retrieve !Key !(Reply (Maybe ByteString))
  | Delete !Key

data Contents =
    None                  -- Store key with no contenst (e.g. directory)
  | Raw  !ByteString      -- Read content from memory
  | File !FilePath        -- Read content from file
  deriving Show

recover hset rollback statCh kidxCh hidxCh = do
  ex <- doesDirectoryExist rollback
  when ex $ do
    send statCh $ Stats.Say "  Checking index consistency!"
    join $ sendReply kidxCh $ Idx.Mapi_ f
    flushChannel kidxCh
  where
    f key (mbvs, metaid, ids) = do
      send statCh $ Stats.SetMessage $ B.unpack key :: IO ()
      -- xs <- forM (metaid : ids) $ sendReply hidxCh . Idx.Lookup
      when (any (`Set.member` hset) $ metaid:ids) $
        send kidxCh $ Idx.Delete key

keyStore idxCh hsCh = new (handleSup, handleMsg, info "Started", hFlush)
  where
    handleSup msg =
      case msg of
        Stop -> info $ "Goodbye"
        _ -> warning $ "Ignoring supervisor message: " ++ show msg

    handleMsg (Delete key) =
      send idxCh $ Idx.Delete key

    handleMsg (Insert mbvs key mmeta content rep) = {-# SCC "keyStore" #-} do
      mb <- sendReply idxCh $ Idx.Lookup key
      case mb of
        Nothing -> go
        Just (mbvs', _, _) -> do
          let exists = fromMaybe False $ do vs <- mbvs
                                            vs' <- mbvs'
                                            return $ vs == vs'
          if exists then replyTo rep $ Right False
            else go
        where
          go = do
            let msource =
                  case content of
                    None      -> Nothing
                    File path -> Just $ CB.sourceFile path
                    Raw bytes -> Just $ CL.sourceList [bytes]
            info $ "Insert: " ++ show key
            mids <- case msource of
              Just source -> liftIO $ do
                -- liftIO $ run (enum $= chunker sum $$ go [])
                Right `fmap` runResourceT
                  (source
                   $= chunker
                   -- $= CL.mapM (\chunk -> do
                   --                print $ B.length chunk
                   --                return chunk
                   --            )
                   $= CL.mapM (sendReply hsCh . HS.Insert)
                   $$ CL.consume
                  )
                `catch` \(e :: SomeException) -> return $ Left e
              Nothing -> return $ Right []
            case mids of
              Left e -> do
                warning $ concat $ ["Could not insert key: ", show key, "\n", show e]
                replyTo rep $ Left $ show e
              Right ids -> do
                meta <- liftIO $ mmeta
                metaid <- sendReply hsCh $ HS.Insert meta
                send idxCh $ Idx.Insert key (mbvs, metaid, ids)
                replyTo rep $ Right True

    handleMsg (Retrieve k rep) = do
      mb <- sendReply idxCh $ Idx.Lookup k
      case mb of
        Nothing -> replyTo rep Nothing
        Just (_, metaid, ids) -> do
          chunks <- mapM (sendReply hsCh . HS.Lookup) $ ids
          replyTo rep $ B.concat `fmap` sequence chunks

    hFlush = flushChannel hsCh

maxChunkSize :: Int
maxChunkSize = 2^16

minChunkSize :: Int
minChunkSize = 2^12

chunker =
  case chunkMode of
    RSync -> rsyncChunker
    Fixed -> fixedChunker

fixedChunker = conduitState B.empty push close
  where
    push state input = do
      let chunk = B.append state input
          size  = B.length chunk
      if size >= maxChunkSize
        then do
        let (output', state') = B.splitAt (fromIntegral maxChunkSize) chunk
            !output = B.copy output'
        return $ StateProducing state' [output]
        else
        return $ StateProducing chunk []
    close state = do
      let !output = B.copy state
      return [output]

-- rsyncChunker = Conduit $ do
--   sum <- lift makeSum
--   let feed  = feedSum sum
--       reset = resetSum sum
--       split = splitBlock sum
--   istate <- newRef B.empty
--   return $ PreparedConduit {
--     conduitPush = \input -> do
--        state <- readRef istate
--        (state', res) <- state `seq` (lift $ push feed reset split state input)
--        writeRef istate state'
--        return res
--     ,
--     conduitClose =
--        readRef istate >>= close
--     }

rsyncChunker =
  Conduit
  { conduitPush = \input -> do
       sum <- lift makeSum
       let feed  = feedSum sum
           reset = resetSum sum
           split = splitBlock sum
       push feed reset split B.empty input
  , conduitClose = close0 B.empty
  }
  where
    push feed reset split state input = do
      res <- state `seq` (lift $ push0 feed reset split state input)
      return $ case res of
        StateFinished a b -> Finished a b
        StateProducing state' output ->
          Producing
          (Conduit (push feed reset split state') (close0 state'))
          output

    push0 feed reset split state input
      -- Too small
      | size < minChunkSize = do
        feed input
        return $ StateProducing (B.append state input) []
      -- Too large
      | size >= maxChunkSize = do
          let chunk = B.append state input
              (output', state') = B.splitAt (fromIntegral maxChunkSize) chunk
              !output = B.copy output'
          reset
          feed state'
          return $ StateProducing state' [output]
      -- Try chunking
      | otherwise = do
        msplit <- split input
        case msplit of
          Just (output', state') -> do
            let !output = B.copy $ B.append state output'
            reset
            feed state'
            return $ StateProducing state' [output]
          Nothing ->
            return $ StateProducing (B.append state input) []
      where
        size = B.length state
    close0 state = do
      let !output = B.copy state
      return [output]
