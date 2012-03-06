module Supervisor
       ( SupRead
       , SupWrite
       , SupMessage (..)
       , CliMessage (..)
       )
       where

import Channel

type SupRead = ReadChannel SupMessage
type SupWrite = WriteChannel CliMessage

data SupMessage
  = Stop
  | Status
  deriving (Show, Eq)

type PID = String
type Name = String
data CliMessage
  = Register (WriteChannel SupMessage) PID Name
  | Done String
