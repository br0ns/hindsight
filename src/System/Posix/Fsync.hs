{-# LANGUAGE ForeignFunctionInterface #-}
module System.Posix.Fsync (fsync, sync, syncFile) where

import Foreign.C.Error    (throwErrnoIfMinus1_)
import Foreign.C.Types    (CInt(..))

import System.FilePath

import System.IO          (withFile, Handle, IOMode(..))
import System.Posix.IO    (handleToFd, closeFd)
import System.Posix.Types (Fd(..))

foreign import ccall safe fsync :: CInt -> IO CInt

fsync' :: Fd -> IO ()
fsync' (Fd fd) = throwErrnoIfMinus1_ "fdatasync" $ fsync fd

sync :: Handle -> IO ()
sync h = handleToFd h >>= \fd -> do fsync' fd
                                    closeFd fd

syncFile :: FilePath -> IO ()
syncFile path = withFile path ReadMode sync