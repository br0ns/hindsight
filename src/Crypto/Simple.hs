{-# LANGUAGE ScopedTypeVariables
  #-}

module Crypto.Simple (newMasterKey, readMasterKey, encrypt, decrypt)
       where

import qualified Crypto.NaCl.Encrypt.Stream as NaCl
import qualified Crypto.NaCl.Nonce          as Nonce

import Data.ByteString.Char8 as B
import Data.Serialize as S
import Data.Word

import System.Entropy (getEntropy)


import Crypto.Classes (hash')
import Crypto.HMAC (MacKey(..), hmac')

import Crypto.Skein (Skein_256_256)
import Crypto.Scrypt (scrypt')


type PlainText  = ByteString
type CipherText = ByteString


{-| Key types -}
newtype MasterKey = MasterKey { unMasterKey :: ByteString }


instance S.Serialize MasterKey where
  get = MasterKey `fmap` S.getByteString NaCl.keyLength
  put = S.putByteString . unMasterKey


newtype Passphrase  = Passphrase  { unPassphrase  :: ByteString }
newtype ExportedKey = ExportedKey { unExportedKey :: ByteString }
newtype Key         = Key         { unKey         :: ByteString }


newMasterKey :: FilePath -> IO ()
newMasterKey path =
  do key <- MasterKey `fmap` getEntropy NaCl.keyLength
     B.writeFile path $ encode key


readMasterKey :: FilePath -> IO (Either String MasterKey)
readMasterKey path =
  S.decode `fmap` B.readFile path


encrypt :: MasterKey -> ByteString -> IO ByteString
encrypt (MasterKey key) plainT = cipherT
  where
    cipherT = do
      nonce <- Nonce.createRandomNonce NaCl.nonceLength
      return $ encode (Nonce.toBS nonce, NaCl.encrypt nonce plainT key)


decrypt :: MasterKey -> CipherText -> Either String PlainText
decrypt (MasterKey key) cipherT =
  case decode cipherT of
    Left  s                   -> Left s
    Right (bsNonce, bsCipher) ->
      Right $ NaCl.decrypt (Nonce.fromBS bsNonce) bsCipher key
