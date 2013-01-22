{-# LANGUAGE GeneralizedNewtypeDeriving
           , RecordWildCards
  #-}

module Data.SuffixTree.Types
       ( SuffixNode(..)
       , SuffixMeta(..)
       , SuffixTreeM(..)
       , StRead(..)
       , StState(..)
       , Key
       , Value
       , NodeID(..)
       , runSuffixTree
       )
       where


-- | Imports

import Data.Word       (Word64)
import Data.ByteString (ByteString)
import Data.Serialize  (Serialize(..))

import Control.Monad.State  hiding (get, put)
import Control.Monad.Reader

import Data.SuffixTree.Interval


-- | Types

type Key    = ByteString
type Value  = ByteString


newtype NodeID = NodeID { unNodeID :: ByteString }
               deriving (Show, Eq)

data SuffixNode = StBranch
                  { branchKeys  :: Interval Key NodeID }
                | StLeaf
                  { leafPrefixes :: [(Key, NodeID)]
                  , leafKeys     :: [(Key, Value)]
                  }


data SuffixMeta = StMeta
                  { nodeData :: SuffixNode
                  , nodeID   :: NodeID
                  }


data StRead  = StRead
               { stDir      :: FilePath
               , stPrefixer :: ByteString -> ByteString -> ByteString
               }


data StState = StState
               { stRoot     :: SuffixMeta
               , stNextID   :: Word64
               , stFree     :: [NodeID]
               }


newtype SuffixTreeM a = SuffixTreeM {
  runSuffixTreeM :: ReaderT StRead (StateT StState IO) a
  } deriving (Functor, Monad, MonadIO, MonadReader StRead, MonadState StState)


-- | Instances

instance Serialize NodeID where
  put = put . unNodeID
  get = liftM NodeID get

instance Serialize SuffixNode where
  put StBranch{..} = put 'b' >> put branchKeys
  put StLeaf{..}   = put 'l' >> put leafPrefixes >> put leafKeys

  get = do
    tag <- get
    case tag of
      'b' -> liftM  StBranch get
      'l' -> liftM2 StLeaf   get get


-- | Runners

runSuffixTree dir root nextID prefixer treeM = runStateT (runReaderT m r) st
  where
    st = StState root nextID []
    r  = StRead dir prefixer
    m  = runSuffixTreeM treeM
