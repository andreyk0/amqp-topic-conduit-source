{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell            #-}

module Main where

import           Control.Monad.Trans.Resource
import           Data.Conduit
import qualified Data.Conduit.Combinators     as C
import qualified Data.Text                    as T
import           Network.AMQP
import           Network.AMQP.Topic.Source
import           RIO
import           System.Environment



main :: IO ()
main = do
  as <- getArgs
  (amqpUrl, rKey) <- case as
                       of [u,k] -> return (u, T.pack k)
                          _     -> error "Usage: example 'amqp://guest:guest@amqp:5672/' '.something.or.other.#'"

  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \ lf -> do
    runRIO lf $ runResourceT . runConduit $ sourceEventsC amqpUrl rKey .| printMsg 5


printMsg:: (Monad m, MonadIO m)
        => Int -- ^ num messages
        -> ConduitT (Message,Envelope) Void m ()
printMsg i =
  C.takeExactly i $ awaitForever (\(msg,_) -> liftIO $ putStrLn $ "Received message: " <> (show.msgBody) msg)
