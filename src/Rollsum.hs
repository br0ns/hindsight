{-# LANGUAGE ForeignFunctionInterface
           , EmptyDataDecls
   #-}

module Rollsum where

import Data.Word

import qualified Data.ByteString      as B
import qualified Data.ByteString.Lazy as BL

import Data.ByteString.Internal as BI

import Foreign
import Foreign.C


blobModulo = 16383


foreign import ccall unsafe "rollsum.h"
  c_make :: IO (Ptr CSum)

foreign import ccall unsafe "rollsum.h"
  c_init :: Ptr CSum -> IO ()

foreign import ccall unsafe "rollsum.h"
  c_next :: Ptr CSum -> Ptr Word8 -> CInt -> CUInt -> IO CInt

foreign import ccall unsafe "rollsum.h"
  c_feed :: Ptr CSum -> Ptr Word8 -> CInt -> CUInt -> IO ()


data CSum
newtype Sum = Sum { getSum :: ForeignPtr CSum }
            deriving Show

makeSum :: IO Sum
makeSum = do
  cs <- c_make
  fptr <- newForeignPtr finalizerFree cs
  return $! Sum $! fptr
{-# INLINE makeSum #-}


resetSum :: Sum -> IO ()
resetSum (Sum fptr) = withForeignPtr fptr c_init
{-# INLINE resetSum #-}


nextPos :: Sum -> B.ByteString -> IO (Maybe Int)
nextPos (Sum fptrS) bs = do
  withForeignPtr fptrB $ \ptrB -> do
  withForeignPtr fptrS $ \ptrS -> do
    pos <- c_next ptrS (ptrB `plusPtr` offset) (fromIntegral len) blobModulo
    return $ if pos == -1
             then Nothing
             else Just $! fromIntegral pos
  where
    (fptrB, offset, len) = toForeignPtr bs
{-# INLINE nextPos #-}

splitBlock :: Sum -> B.ByteString -> IO (Maybe (B.ByteString, B.ByteString))
splitBlock sum bs = do
  mpos <- nextPos sum bs
  return $ do pos <- mpos
              return $ B.splitAt pos bs
{-# INLINE splitBlock #-}


splitBlockLazy :: Sum -> BL.ByteString -> IO (Maybe (BL.ByteString, BL.ByteString))
splitBlockLazy sum bs = go BL.empty $ BL.toChunks bs
  where
    go s []     = return Nothing
    go s (x:xs) = do
      mpos <- nextPos sum x
      case mpos of
        Just i -> let len = BL.length s + fromIntegral i
                  in  return $ Just $! BL.splitAt len bs
        Nothing     -> go (BL.append s $ BL.fromChunks [x]) xs
{-# INLINE splitBlockLazy #-}


feedSum :: Sum -> B.ByteString -> IO ()
feedSum sum = go
  where
    go bs = do
      msplit <- splitBlock sum bs
      case msplit of
        Nothing       -> return ()
        Just (hd, tl) -> go tl
{-# INLINE feedSum #-}


feedSumLazy :: Sum -> BL.ByteString -> IO ()
feedSumLazy sum = mapM_ (feedSum sum) . BL.toChunks
