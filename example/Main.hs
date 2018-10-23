{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell            #-}

module Main where

import           Control.Monad.Logger
import           Control.Monad.Reader
import           Control.Monad.Trans.Resource
import           Data.Conduit
import qualified Data.Conduit.Combinators as C
import qualified Data.Text as T
import           Network.AMQP
import           Network.AMQP.Topic.Source
import           System.Environment
import           System.IO


-- | Toy example of an app stack with MonadLoggerIO.
newtype EgApp a = EgApp { unEgApp:: ReaderT String (ResourceT (LoggingT IO)) a
                        } deriving ( Applicative
                                   , Functor
                                   , Monad
                                   , MonadIO
                                   , MonadLogger
                                   , MonadLoggerIO
                                   , MonadReader String
                                   , MonadResource
                                   )
-- | Run toy app.
runEgApp:: String
        -> EgApp a
        -> IO a
runEgApp s app =
  runStderrLoggingT $ runResourceT $ runReaderT (unEgApp app) s


main :: IO ()
main = do
  as <- getArgs
  (amqpUrl, rKey) <- case as
                       of [u,k] -> return (u, T.pack k)
                          _     -> error "Usage: example 'amqp://guest:guest@amqp:5672/' '.something.or.other.#'"

  hSetBuffering stderr LineBuffering

  runResourceT . runConduit $ sourceEvents amqpUrl rKey .| printMsg 5 -- logs to stderr without MonadLogger

  runResourceT . runConduit $ sourceEventsL noopEventSourceLogger amqpUrl rKey .| printMsg 7 -- runs quietly

  runEgApp "test" $ do x <- ask
                       $(logInfo) $ "Running with " <> T.pack x
                       runConduit $ sourceEventsML amqpUrl rKey .| printMsg 3

  putStrLn "ALL DONE"


printMsg:: (Monad m, MonadIO m)
        => Int -- ^ num messages
        -> ConduitT (Message,Envelope) Void m ()
printMsg i =
  C.takeExactly i $ awaitForever (\(msg,_) -> liftIO $ putStrLn $ "Received message: " <> (show.msgBody) msg)
