{-# LANGUAGE ScopedTypeVariables
           , TypeSynonymInstances
  #-}

import Prelude hiding (lookup)

import Control.Concurrent
import Control.Monad
import Control.Monad.Trans

import Util
import Data.Word

import qualified Data.ByteString.Char8 as B

import qualified Data.BTree.Types           as Ty
import qualified Data.BTree.BTree           as T
import qualified Data.BTree.Cache.Class     as C
import qualified Data.BTree.Cache.STM       as Cstm
import qualified Data.BTree.KVBackend.Files as Files

import qualified Data.Map as M
import Data.Maybe

import Test.QuickCheck
import Test.QuickCheck.Monadic as QCM

import System.Unix.Directory


type Index = Word64
type Key   = Word32
type Value = Word16


data TreeOps = Insert       Key  Value
             | InsertMany [(Key, Value)]

             | UpdateIndex Index Value

             | ModifyKey   ModFun Key   Value
             | ModifyIndex ModFun Index Value

             | ModifyMany [(ModFun, Key, Value)]

             | UpdateNoop  Index

             | DeleteKey   Key
             | DeleteIndex Index
             | DeleteMany [Index]

             | DeleteMin
             | DeleteMax
             deriving (Show)


data ModFun = ModFun (Value -> Value -> Value)


instance T.Interval Key where
  between a b = a + ((b - a) `div` 2)


instance Show ModFun where
  show _ = "ModFun"

instance Arbitrary ModFun where
  arbitrary = ModFun `fmap` (oneof $ map return [
                                  (+)
                                , (*)
                                , (-)
                                , const
                                , flip const
                                ])


smallArbitrary :: Arbitrary a => Gen a
smallArbitrary = sized $ \s -> resize (floor $ sqrt $ fromIntegral s) arbitrary

instance Arbitrary TreeOps where
  arbitrary = oneof [
      liftM2 Insert      arbitrary arbitrary
    , liftM  InsertMany  smallArbitrary

    , liftM3 ModifyKey   arbitrary arbitrary arbitrary
    , liftM3 ModifyIndex arbitrary arbitrary arbitrary
    -- , liftM  ModifyMany  arbitrary

    , liftM2 UpdateIndex arbitrary arbitrary
    , liftM  UpdateNoop  arbitrary

    , liftM  DeleteIndex arbitrary
    , liftM  DeleteKey   arbitrary

    -- , return DeleteMin
    -- , return DeleteMax
    ]


-- behaves_like_map_prop :: String -> Property
behaves_like_map_prop dir root =
  QCM.monadicIO $
  do ops <- pick arbitrary
     eq  <- QCM.run $ interp ops
     assert eq

  where
    interp ops =
      do c <- Cstm.sizedParam 8 $ Files.evalFilesKV dir
         r <- readMVar root
         p <- T.makeParam 8 (Just r) c

         l0 <- T.execTree p $ T.toList

         reb <- T.rebalanceProcess p
         sid <- safeForkIO $ \mv -> forever $ do
                                    threadDelay $ 10^5
                                    withMVar mv $ const $
                                       modifyMVar root $ \_ -> do
                                         r <- T.execTree p $ T.save
                                         return (r, ())
         fid <- safeForkIO $ \mv -> forever $ do
                                    threadDelay $ 10^5
                                    withMVar mv $ const $
                                       Cstm.flush c


         m <- T.execTree p $ foldM go (M.fromList l0) ops

         mapM_ safeKill [reb, sid, fid]
         l1 <- T.execTree p $ T.toList

         return $ M.toList m == l1

    go m op =
      case op of
        Insert k v -> do T.modify const k v
                         return $ M.insert k v m

        InsertMany ks -> do
          mapM_ (uncurry $ T.modify const) ks
          return $ foldl (flip $ uncurry $ M.insert) m ks


        ModifyKey (ModFun f) k v -> do
          T.modify f k v
          return $ M.insertWith f k v m

        ModifyIndex (ModFun f) idx v -> withKey idx $ \k -> do
          T.modify f k v
          return $ M.insertWith f k v m

        ModifyMany fkvs -> do
          mapM_ (\(ModFun f, k, v) -> T.modify f k v) fkvs
          return $ foldl (\m (ModFun f, k, v) -> M.insertWith f k v m) m fkvs

        UpdateIndex idx v -> withKey idx $ \k -> do
          T.modify const k v
          return $ M.insert k v m

        UpdateNoop idx -> withKey idx $ \k -> do
          T.modify (flip const) k $ fromIntegral k
          return m

        DeleteIndex idx -> withKey idx $ \k -> do
          T.delete k
          return $ M.delete k m

        DeleteKey k -> do
          T.delete k
          return $ M.delete k m

        -- DeleteMin
        --   | M.null m  -> return m
        --   | otherwise -> do
        --     (k, _) <- T.findMin
        --     T.delete k
        --     return $ M.deleteMin m

        _ -> return m

      where
        withKey idx f
          | M.null m  = return m
          | otherwise = do
            f $ fst $ M.elemAt (fromIntegral idx `mod` M.size m) m


main = do
  let args = stdArgs {maxSuccess = 100, maxSize = 1000}
  withTemporaryDirectory "hsbackup-" $ \dir ->
    do c <- Cstm.sizedParam 32 $ Files.evalFilesKV dir
       p <- T.makeParam 16 Nothing c
       r <- T.execTree p T.save
       root <- newMVar r
       quickCheckWithResult args $ behaves_like_map_prop dir root
