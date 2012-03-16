module Data.Conduit.Extra
       ( group
       )
       where

import Data.Conduit

group :: Resource m => Int -> Conduit a m [a]
group n = conduitState
    st0
    push
    close
  where
    close (_, xs) = return [xs]
    push (m, xs) x = do
      let n' = m + 1
      return $ if n' == n
               then StateProducing st0 [reverse $ x : xs]
               else StateProducing (n', x : xs) []
    st0 = (0, [])