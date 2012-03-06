{-# LANGUAGE FlexibleInstances
           , UndecidableInstances
  #-}

module Data.Pickle
       ( encode
       , decode
       , Picklable(..))
       where


import Data.ByteString
import qualified Data.Serialize as S


class Picklable a where
  dumps :: a -> ByteString
  loads :: ByteString -> Either String a

-- aliases
encode :: Picklable a => a -> ByteString
encode = dumps

decode :: Picklable a => ByteString -> Either String a
decode = loads


-- Serialize is picklable
instance S.Serialize a => Picklable a where
  dumps = S.encode
  loads = S.decode
