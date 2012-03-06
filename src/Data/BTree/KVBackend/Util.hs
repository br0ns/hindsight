module Data.BTree.KVBackend.Util where


bsToFilePath :: S.Serialize k => k -> FilesKV FilePath
bsToFilePath path =
  do dir <- ask
     return $ dir </> (B.unpack $ B.map fix $ B64.encode $ S.encode path)
  where
    fix '/' = '-'
    fix c   = c
