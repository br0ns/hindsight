module Config where


-- The module to use for external storage
-- See example modules in the "modules/" directory
backendModule = "~/.hindsight/modules/local"

-- Debugging (default: DebugOff)
data Debug = DebugOn | DebugOff
           deriving Eq

debugMode = DebugOff


-- Chunking of files (default: RSync)
data ChunkMode = Fixed | RSync
               deriving Eq

chunkMode = Fixed
-- chunkMode = RSync


-- Compressions of blobs (default: Snappy)
-- (This will not affect internal compression of tree nodes)
data CompressionMode = CompressionOff | Snappy
                     deriving Eq

compressionMode = Snappy
