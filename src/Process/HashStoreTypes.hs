module Process.HashStoreTypes
       ( ID
       , Value
       , Message (..)
       )
       where

import Data.Word
import Data.Serialize as Ser
import Crypto.Flat (Skein256)
import Data.ByteString (ByteString)
import Process
import qualified Process.BlobStoreTypes as BS

-- ID
type ID = Skein256

-- Message
type Value   = ByteString
data Message
  = Insert !Value !(Reply ID)
  | InsertBlob !ID !BS.ID !(Reply ())
  | Lookup !ID !(Reply (Maybe Value))
