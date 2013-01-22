{-# LANGUAGE GeneralizedNewtypeDeriving
           , OverloadedStrings
  #-}

module Data.SuffixTree
       ( SuffixTreeM
       , runSuffixTree
       , runNew
       , runWith

       , buildPrefixer
       , defaultPrefixer

       , initT
       , insertT
       , lookupT

       , saveT
       , printT
       )
       where

import qualified Data.List as L
import qualified Data.ByteString.Char8  as B
import qualified Data.ByteString.Base16 as B16

import qualified Data.Serialize as Ser

import Data.Ord      (comparing)
import Data.Maybe    (listToMaybe, fromJust, isJust)
import Data.Function (on)

import System.FilePath ((</>))
import System.Directory (removeFile)

import Control.Applicative
import Control.Arrow
import Control.Monad
import Control.Monad.Trans

import qualified Control.Monad.State  as ST
import qualified Control.Monad.Reader as R

import           Data.SuffixTree.Types
import qualified Data.SuffixTree.Interval as I


-- | Constants
_order     = 128
_minPrefix = _order `div` 2


-- | Small helpers

buildPrefixer :: Char -> B.ByteString -> B.ByteString -> B.ByteString
buildPrefixer chr bs0 bs1 | B.null prefix = B.empty
                          | otherwise     = B.concat [prefix, B.singleton '/']
  where
    prefix = B.intercalate (B.singleton chr) $ map fst $
             takeWhile (uncurry (==)) $ zip split0 split1
    split0 = B.split chr bs0
    split1 = B.split chr bs1

defaultPrefixer = buildPrefixer '/'


formatID :: NodeID -> String
formatID = B.unpack . B16.encode . unNodeID


tryPackLeaf :: SuffixNode -> SuffixTreeM (Maybe SuffixNode)
tryPackLeaf (StLeaf ps ls) = do
  prefixer <- R.asks stPrefixer
  let cands = calcPrefixes prefixer
  case L.sortBy (comparing $ negate . fst) cands of
    ((count, prefix) :_) | count >= _minPrefix -> do
      let (inner, outer) = L.partition ((prefix `B.isPrefixOf`) . fst) ls
      let inner2 = map (first $ stripPrefix prefix) inner
      leaf <- writeNew $ StLeaf [] inner2
      return $ Just $ StLeaf ((prefix, nodeID leaf):ps) outer

    _ -> return Nothing
  where
    keys = L.sort $ map fst ls
    calcPrefixes p = go keys
      where
        go (a:b:ks) = case a `p` b of
          s | B.null s  -> go $ b:ks
            | otherwise -> (length $ takeWhile (s `B.isPrefixOf`) ks, s) : go (b:ks)
        go _ = []

stripPrefix p = B.drop $ B.length p



-- | Internal functions

-- Split a leaf into a branch node with two references
splitLeaf (StLeaf ps ls) = do
  left  <- writeNew $ StLeaf psLeft  lsLeft
  right <- writeNew $ StLeaf psRight lsRight
  return $ StBranch $ I.singleton (nodeID left, mid, nodeID right)
  where
    (psLeft, psRight) = split ps
    (lsLeft, lsRight) = split ls

    keys = L.sort $ map fst ps ++ map fst ls

    mid1:mid2:_ = drop ((length keys `div` 2) - 1) keys
    mid = B.take (1 + length (takeWhile (uncurry (==)) $ B.zip mid1 mid2)) mid2

    split = L.partition ((<mid) . fst)


-- Split a branch into a branch node with two references
splitBranch (StBranch it) = do
  left  <- writeNew $ StBranch $ leftI
  right <- writeNew $ StBranch $ rightI
  return $ StBranch $ I.singleton (nodeID left, mid, nodeID right)
  where
    (leftI, mid, rightI) = I.split it


-- Root setters
setRoot :: SuffixMeta -> SuffixTreeM ()
setRoot root = ST.modify $ \s -> s { stRoot = root }

setRootNode :: SuffixNode -> SuffixTreeM ()
setRootNode rootData = do
  root <- getRoot
  setRoot $ root { nodeData = rootData }

-- Root getters
getRoot :: SuffixTreeM SuffixMeta
getRoot = ST.gets stRoot

getRootNode :: SuffixTreeM SuffixNode
getRootNode = nodeData <$> getRoot


-- Get next node id (unique from counter)
nextID :: SuffixTreeM NodeID
nextID = do
  free <- ST.gets stFree
  nodeID <- case free of
    (x:xs) -> ST.modify (\s -> s { stFree = xs }) >> return x
    []     -> do
      ST.modify $ \s -> s { stNextID = stNextID s + 1 }
      ST.gets $ NodeID . Ser.encode . stNextID
  return nodeID


remove :: SuffixMeta -> SuffixTreeM ()
remove meta = do
  dir <- R.asks stDir
  liftIO $ removeFile $ dir </> formatID (nodeID meta)
  ST.modify $ \s -> s { stFree = (nodeID meta) : stFree s }

-- Write a node with meta info to disk
write :: SuffixMeta -> SuffixTreeM ()
write meta = do
  dir <- R.asks stDir
  let filename = dir </> formatID (nodeID meta)
      contents = nodeData meta
  liftIO $ B.writeFile filename (Ser.encode contents)


-- Write a node to disk and return its new meta node
writeNew :: SuffixNode -> SuffixTreeM SuffixMeta
writeNew node = do
  meta <- liftM (StMeta node) nextID
  write meta
  return meta

-- Temporarily shift to a new root and perfom an action in this state
withRoot :: NodeID -> SuffixTreeM a -> SuffixTreeM (SuffixMeta, a)
withRoot rootID treeM = do
  -- Current state
  r  <- R.ask
  st <- ST.get
  -- Run action to get result and new state
  (a, st2) <- liftIO $ runWith (stDir r) (stPrefixer r) rootID (stNextID st)
              treeM
  -- Update ID counter
  ST.modify $ \s -> s { stNextID = stNextID st2 }
  -- Give back possibly new nodeID and result value
  return (stRoot st2, a)

withRoot_ rootID treeM = fst <$> withRoot rootID treeM



-- | Exported functions


-- Create a new tree and work with it
runNew dir prefixer treeM = runSuffixTree dir undefined 0 prefixer $
                            initT >> treeM >>= (\v -> saveT >> return v)


-- Open an existing tree and work with it
runWith dir prefixer rootID nextID treeM = do
  eroot <- Ser.decode <$> B.readFile (dir </> formatID rootID)
  case eroot of
    Left  err      -> error $ "Could not open root node: " ++ err
    Right rootNode -> do
      let root = StMeta rootNode rootID
      runSuffixTree dir root nextID prefixer treeM


-- Save current root to disk
saveT :: SuffixTreeM ()
saveT = write =<< getRoot


-- Initialise empty tree
initT :: SuffixTreeM ()
initT = do
  root <- writeNew $ StLeaf [] []
  setRoot root


-- Lookup value
lookupT :: Key -> SuffixTreeM (Maybe Value)
lookupT k = go
  where
    go = do
      root <- getRootNode
      case root of
        StLeaf ps ls -> do
          case [ p | p@(prefix,_) <- ps, prefix `B.isPrefixOf` k ] of
            [] -> return $ listToMaybe [ v | (k', v) <- ls, k' == k ]
            ((prefix, childID):_) -> do
              snd <$> withRoot childID (lookupT $ stripPrefix prefix k)
        StBranch it -> do
          let childID = I.lookup k it
          snd <$> withRoot childID go


-- Insert key-value pair
insertT :: Key -> Value -> SuffixTreeM ()
insertT k v = go
  where
    retry = saveT >> go
    go = do
      root <- getRootNode
      case root of
        -- insert into leaf
        StLeaf ps ls | (length ps + length ls) < _order -> do
          case [ p | p@(prefix,_) <- ps, prefix `B.isPrefixOf` k ] of
            [] -> setRootNode $ StLeaf ps ((k,v) : filter ((/=k).fst) ls)

            ((prefix, childID):_) -> do
              subMeta <- withRoot_ childID $ insertT (stripPrefix prefix k) v
              setRootNode $
                StLeaf ((prefix, nodeID subMeta) : filter ((/=prefix).fst) ps) ls

          saveT

        -- split leaf
        StLeaf ps ls -> do
          mpacked <- tryPackLeaf root
          case mpacked of
            Just packed -> setRootNode packed
            Nothing     -> setRootNode =<< splitLeaf root
          retry

        -- split branch
        StBranch it | I.size it > _order -> do
          setRootNode =<< splitBranch root
          retry

        -- search branch and merge child if necessary
        StBranch it -> do
          let (childKey, childID) = I.lookupPair k it
          subMeta <- withRoot_ childID go
          let mupdate = case nodeData subMeta of
                StBranch subIt
                  | I.size subIt == 1 -> Just $ I.insert (I.range subIt) it
                  | childID /= nodeID subMeta ->
                    Just $ I.replace (childKey, nodeID subMeta) it

                _ -> Nothing

          case mupdate of
            Nothing     -> return ()
            Just update -> setRootNode (StBranch update) >>
                           remove subMeta >> saveT


printT :: SuffixTreeM ()
printT = go $ liftIO . putStrLn
  where
    go out = do
      root <- getRootNode
      case root of
        StLeaf ps ls -> do
          out "Leaf: "
          let out' = out . ("|_"++)
          mapM_ (out'.show.fst) ls
          forM_ ps $ \(prefix, child) -> do
            out' $ "Prefix " ++ show prefix ++ ":"
            withRoot_ child $ go $ out . ("    "++)

        StBranch it -> do
          out $ "Branch: " ++ show (I.size it)
          forM_ (I.toList it) $ \(mkey, child) -> do
            when (isJust mkey) $
              out $ "|> " ++ show (fromJust mkey) ++ ":"
            withRoot_ child $ go $ out . ("| "++)
          out "|_|"


