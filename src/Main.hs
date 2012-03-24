{-# LANGUAGE ScopedTypeVariables
  #-}
module Main
       where

import Prelude hiding (catch)
import Control.Exception

import System.FilePath
import System.Environment
import System.Directory
import qualified Data.ByteString as B

import Config
import Backup
import Process.Log as L
import Data.List
import Control.Arrow (second)
import Control.Monad.Error
import Control.Concurrent

import Data.UUID
import Data.ByteString.Char8 (pack, unpack)
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Char8 as B8
import Data.ByteString (ByteString)
import Data.Serialize (Serialize(..), encode)
import Util hiding (seal)

usage = do
  name <- getProgName
  mapM_ putStrLn
    [ "Usage: " ++ name ++ " <command>"
    , "  Commands:"
    , "    init"
    , "      Initialise the system. Generates a new key file in <hindsightBase>."
    , "      <hindsightBase> can be configured in `Config.hs'."
    , ""
    , "    snapshot <name> <path>"
    , "      Take a new snapshot of <path> and save it under the name <name>."
    , ""
    , "    checkout <src> <dst> [-r]"
    , "      Unpack (parts of) a snapshot to the folder <dst>. If <src> is a name or"
    , "      a name and a version the whole snapshot is unpacked. Else the default"
    , "      behavior is non-recusive unpacking, which may be overruled with the `-r'"
    , "      flag."
    , ""
    , "    seal"
    , "      Saves the internal state remotely. You should run this command often. The"
    , "      `recover' command will reset the system to it's state at the time of the"
    , "      last `seal'."
    , ""
    , "    recover"
    , "      Recover the system state to the time of the last `seal' command. Note that"
    , "      the system key is needed."
    , ""
    , "    search <src>"
    , "      Search for entries within a snapshot. Lists all entries of which the"
    , "      keyword is a substring."
    , ""
    , "    list [<src>]"
    , "      List entries in a snapshot. If no argument is given a list of all"
    , "      available snapshots is printed. Else list every entry of which keyword is"
    , "      a prefix."
    , ""
    , "    listdir <src>"
    , "      List entries of a directory non-recursively."
    , ""
    , "  Source descriptor (<src>):"
    , "    The first argument of the commands `checkout', `search' and `list' describes"
    , "    one or more entries in a snapshot. It has the format"
    , ""
    , "      <name>[~<version>][:<keyword>]"
    , ""
    , "    The first snapshot has version 1, the next version 2 and so on. The meaning"
    , "    of the <keyword> depends on the function; `checkout` assumes it is the path"
    , "    of a file or directory, `list' looks for entries with <keyword> as a prefix"
    , "    and `search' looks for entries that has it as a substring."
    , ""
    , "  Examples:"
    , "    Initialise the system, snapshot the home directory and seal the state:"
    , ""
    , "      " ++ name ++ " init"
    , "      " ++ name ++ " snapshot my-home-dir ~"
    , "      " ++ name ++ " seal"
    , ""
    , "    Checkout everything (recursively) in the folder `foo' of the very first"
    , "    snapshot of `my-home-dir' to directory `bar':"
    , ""
    , "      " ++ name ++ " recover my-home-dir~1:foo -r bar"
    ]
location x =
  (n, maybe 1 read v, d)
    where
      (r, d) = go ':' x
      (n, v) = go '~' r
      go c = second toMaybe . break (c ==)
      toMaybe "" = Nothing
      toMaybe x = Just $ tail x

getFlag f ls = (f `elem` ls, delete f ls)

go = do
  when (debugMode == DebugOn) $
    L.setupLogger L.DEBUG
  args <- getArgs
  case args of
    cmd : args -> do
      home <- getHomeDirectory
      base <- expandUser hindsightBase
      createDirectoryIfMissing True base
      case cmd of
        "init"     -> Backup.init base
        "snapshot" ->
          case args of
            [repo, path] -> snapshot base repo path
            _ -> usage
        "checkout" ->
          case args'' of
            [src, dst] -> checkout rec noData base repo version dir dst
              where
                (repo, version, dir) = location src
            _ -> usage
          where
            (rec, args') = getFlag "-r" args
            (noData, args'') = getFlag "--nodata" args
        "seal" ->
          seal base
        "recover" ->
          recover base
        "search" ->
          case location $ head args of
            (repo, version, Just key) -> search base repo version key
        "list" ->
          case args of
            [] -> listall base
            [src] -> list False base repo version dir
              where
                (repo, version, dir) = location src
            _ -> usage
        "listdir" ->
          case args of
            [src] -> listdir base repo version dir
              where
                (repo, version, Just dir) = location src
            _ -> usage
        "delete" ->
          case location $ head args of
            (repo, version, Nothing) -> deleteSnapshot base repo version
        "hac" ->
          case args of
            ["c", hac, path] -> do
              id <- (("__" ++).byteStringToFileName.encode) `fmap` liftIO uuid
              snapshot base id path
              writeFile hac id
            ["x", hac, path] -> do
              id <- readFile hac
              checkout True False base id 1 Nothing path
        "gc" ->
          collectGarbage base
        "bloom" ->
          case args of
            [src] -> bloomStat base repo version
              where
                (repo, version, Nothing) = location src

        _ -> putStrLn $ "Unknown command: " ++ cmd
    [] -> usage

main = do go `catch` \ (e :: SomeException) -> print e
          -- This is so much of a hack that I won't even tell you what it's for
          threadDelay 100000
