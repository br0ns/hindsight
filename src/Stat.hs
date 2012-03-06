{-# LANGUAGE ScopedTypeVariables #-}

module Stat where

import qualified System.Posix.User      as U
import qualified System.Posix.Files     as F
import qualified System.Posix.Directory as D
import           System.Posix.Types
import           System.Posix.IO

import Prelude hiding (catch)

import Control.Exception
import Control.Monad

import Data.Serialize
import Data.Word

import GHC.ForeignPtr
import Unsafe.Coerce

import qualified Data.ByteString.Internal as BI
import qualified Data.ByteString          as B


newtype CStat a = CStat (ForeignPtr a)

data FileExtra  = FE { ownerName :: String
                     , groupName :: String }

data FileStatus = FS { getFileStatus :: F.FileStatus
                     , getExtra :: FileExtra}


statToFileStatus :: F.FileStatus -> IO FileStatus
statToFileStatus st = do
  uname <- either handler id `fmap` (try $ U.userName  `fmap` U.getUserEntryForID  (F.fileOwner st))
  gname <- either handler id `fmap` (try $ U.groupName `fmap` U.getGroupEntryForID (F.fileGroup st))
  return $ FS st $ FE gname uname
  where
    handler :: SomeException -> String
    handler _ = ""

-- | Compact Serialize instances
instance Serialize FileExtra where
  put (FE o g) = put o >> put g
  get = liftM2 FE get get

instance Serialize FileStatus where
  put fs = do put $ statToBS $ getFileStatus fs
              put $ getExtra $ fs
  get    = do bs <- get
              liftM (FS $ bsToStat bs) get


-- TODO: Use a portable Serialize instance
statSize = 144
statToBS :: F.FileStatus -> B.ByteString
statToBS st = BI.PS ptr 0 statSize
  where
    CStat ptr = unsafeCoerce st

bsToStat (BI.PS ptr 0 144) = unsafeCoerce $ CStat ptr


-- | Read the FileStatus of FilePath
readFileStatus :: FilePath -> IO FileStatus
readFileStatus path = do
  stat <- F.getSymbolicLinkStatus path
  statToFileStatus stat


-- | Write the FileStatus to FilePath (restoring)
writeFileStatus :: FileStatus -> FilePath -> Bool -> IO ()
writeFileStatus status path isLink = do
  unless isLink $ do
    F.setFileTimes     path at mt
    -- Update permissions as the last thing (we need them)
    F.setFileMode      path $ F.fileMode stat
  F.setSymbolicLinkOwnerAndGroup path uid gid
  where
    stat = getFileStatus status
    at   = F.accessTime       stat
    mt   = F.modificationTime stat
    uid  = F.fileOwner stat
    gid  = F.fileGroup stat


data PosixType = File
               | Directory
               | SymLink FilePath
               | BlockDevice
               | CharacterDevice
               | NamedPipe
               | Socket
                 deriving (Show, Eq)

putW :: Putter Word8
putW = put

getW :: Get Word8
getW = get

instance Serialize PosixType where
  put File            = putW 0
  put Directory       = putW 1
  put (SymLink p)     = putW 2 >> put p
  put BlockDevice     = putW 3
  put CharacterDevice = putW 4
  put NamedPipe       = putW 5
  put Socket          = putW 6

  get = do tag <- getW
           case tag of
             0 -> return File
             1 -> return Directory
             2 -> liftM  SymLink get
             3 -> return BlockDevice
             4 -> return CharacterDevice
             5 -> return NamedPipe
             6 -> return Socket



data PosixFile = PosixFile { fileType   :: PosixType
                           , fileStatus :: FileStatus }

instance Serialize PosixFile where
  get = do ft <- get
           fs <- get
           return $! PosixFile ft fs


  put (PosixFile ft fs) = put ft >> put fs



-- | Read the PosixFile descriptor for some path
readPosixFile :: Maybe F.FileStatus -> FilePath -> IO PosixFile
readPosixFile mstat path = do
  stat <- maybe (F.getSymbolicLinkStatus path) return mstat
  ft <- goft stat
  fs <- statToFileStatus stat
  return $ PosixFile ft fs
  where
    goft stat
      | F.isRegularFile     stat = return File
      | F.isDirectory       stat = return Directory
      | F.isSymbolicLink    stat = return . SymLink =<< F.readSymbolicLink path
      | F.isBlockDevice     stat = return BlockDevice
      | F.isCharacterDevice stat = return CharacterDevice
      | F.isNamedPipe       stat = return NamedPipe
      | F.isSocket          stat = return Socket
      | otherwise = error $ "Unknown file type"

isSymLink (SymLink _) = True
isSymLink _           = False

-- | Create file and restore PosixFile descriptor
-- | When the file already exists, it is overridden where possible.
-- | (A directory cannot be overridden with a regular file or link)
-- | Restoring special files are not yet supported (BlockDevice, CharacterDevice, NamedPipe, Socket)
createPosixFile :: PosixFile -> FilePath -> IO ()
createPosixFile (PosixFile ft fs) path = do
  case ft of
    File        -> createFile path (F.fileMode stat) >>= closeFd
    Directory   -> D.createDirectory path (F.fileMode stat)
                   `catch` \(e :: SomeException) -> return ()
    SymLink dest -> F.createSymbolicLink dest path
    other ->
      error $ "Create of " ++ show other ++ " has not yet been implemented."
  writeFileStatus fs path (isSymLink ft)
  where
    stat      = getFileStatus fs
    update fs = writeFileStatus fs path


updatePosixFile :: PosixFile -> FilePath -> IO ()
updatePosixFile pf path =
  writeFileStatus (fileStatus pf) path $ isSymLink $ fileType pf