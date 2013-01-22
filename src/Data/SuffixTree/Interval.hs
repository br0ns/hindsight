{-# LANGUAGE RecordWildCards #-}

module Data.SuffixTree.Interval
       ( Interval
       , singleton
       , range
       , size
       , lookup
       , lookupKey
       , lookupPair
       , insert
       , replace
       , split
       , toList
       ) where


-- | Imports
import           Prelude hiding (lookup)
import           Control.Monad
import qualified Data.List   as L
import           Data.Maybe
import           Data.Serialize


-- | Types
data Interval k v = Interval
                    { iSize :: Int
                    , iKeys :: [(Maybe k, v)] }
                  deriving Show


-- | Instances
instance (Serialize k, Serialize v) => Serialize (Interval k v) where
  put Interval{..} = put iSize >> put iKeys
  get = liftM2 Interval get get


-- | Exported functions
singleton :: (v, k, v) -> Interval k v
singleton (left, k, right) = Interval 1 [ (Nothing, left )
                                        , (Just k,  right) ]

range :: Interval k v -> (v, k, v)
range Interval{..} = (snd $ head iKeys, key, val)
  where
    (Just key, val) = last iKeys


size :: Interval k v -> Int
size = iSize

lookup :: Ord k => k -> Interval k v -> v
lookup k = snd . lookupPair k

lookupKey :: Ord k => k -> Interval k v -> Maybe k
lookupKey k = fst . lookupPair k

lookupPair :: Ord k => k -> Interval k v -> (Maybe k, v)
lookupPair k = last . filter ((<=Just k) . fst) . iKeys

insert :: Ord k => (v, k, v) -> Interval k v -> Interval k v
insert (left, k, right) it = it { iKeys = iKeys2
                                , iSize = iSize it + 1 }
  where
    (psL, psR) = L.partition ((<=Just k) . fst) $ iKeys it
    iKeys2     = init psL ++ [(fst $ last psL, left), (Just k, right)] ++ psR


replace :: Ord k => (Maybe k, v) -> Interval k v -> Interval k v
replace (mk, v) it | L.null right = it
                   | otherwise    = it { iKeys = left ++ (mk, v):tail right }
  where
    (left, right) = L.partition ((<mk) . fst) $ iKeys it


split :: Ord k => Interval k v -> (Interval k v, k, Interval k v)
split Interval{..} | iSize >= 4 = (leftI, midK, rightI)
                   | otherwise  = tooSmallErr

  where
    tooSmallErr = error $ "Interval.split: too small size=" ++ show iSize

    leftI   = Interval (splitPos - 1) left
    rightI  = Interval (length right) ((Nothing, firstR):right)

    (left, (Just midK, firstR):right) = L.splitAt splitPos iKeys

    splitPos = iSize `div` 2


toList :: Interval k v -> [(Maybe k, v)]
toList = iKeys