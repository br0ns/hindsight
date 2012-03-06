#!/usr/bin/env runhaskell

import Control.Monad
import Distribution.Simple
import System.Process

main = defaultMainWithHooks simpleUserHooks { postInst = myPostInst }


myPostInst args flags desc info = do
  -- TODO: Setup modules
  return ()
