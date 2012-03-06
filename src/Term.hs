{-# LANGUAGE ForeignFunctionInterface
           , EmptyDataDecls
   #-}

module Term (
  getTermCols
  ) where


import Foreign.C


foreign import ccall unsafe "term.h"
  c_getTermCols :: IO CInt

getTermCols :: IO Int
getTermCols = do cols <- c_getTermCols
                 return $ fromIntegral cols