{-# LANGUAGE ScopedTypeVariables
           , PatternGuards
  #-}

module Crypto.Simple (newMasterKey, readMasterKey, encrypt, decrypt)
       where

import qualified Crypto.NaCl.Encrypt.Stream as Enc
import qualified Crypto.NaCl.Key            as Key

import Crypto.NaCl.Internal as Nonce(toBS, fromBS)

import Data.ByteString.Char8 as B
import Data.Serialize as S
-- import Data.Word

import System.Entropy (getEntropy)


-- import Crypto.Classes (hash')
-- import Crypto.HMAC (MacKey(..), hmac')

-- import Crypto.Skein (Skein_256_256)


type PlainText  = ByteString
type CipherText = ByteString


{-| Key types -}
newtype MasterKey = MasterKey {
  unMasterKey :: Key.Key Key.Secret Enc.StreamingEncryptionKey }


instance S.Serialize MasterKey where
  get = (MasterKey . Key.Key) `fmap` S.getByteString Enc.keyLength
  put = S.putByteString . Key.unKey . unMasterKey


-- newtype Passphrase  = Passphrase  { unPassphrase  :: ByteString }
-- newtype ExportedKey = ExportedKey { unExportedKey :: ByteString }
-- newtype Key         = Key         { unKey         :: ByteString }


newMasterKey :: FilePath -> IO ()
newMasterKey path =
  do key <- (MasterKey . Key.Key) `fmap` getEntropy Enc.keyLength
     B.writeFile path $ encode key


readMasterKey :: FilePath -> IO (Either String MasterKey)
readMasterKey path =
  S.decode `fmap` B.readFile path


encrypt :: MasterKey -> ByteString -> IO ByteString
encrypt (MasterKey key) plainT = cipherT
  where
    cipherT = do
      nonce <- Enc.randomNonce
      return $ encode (toBS nonce, Enc.encrypt nonce plainT key)


decrypt :: MasterKey -> CipherText -> Either String PlainText
decrypt (MasterKey key) cipherT =
  case decode cipherT of
    Left  s                   -> Left s
    Right (bsNonce, bsCipher)
      | Just nonce <- fromBS bsNonce -> Right $ Enc.decrypt nonce bsCipher key
      | otherwise                    -> Left "Invalid nonce!"
