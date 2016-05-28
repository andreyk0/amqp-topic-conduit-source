{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell            #-}

module Main where

import           Control.Monad.Logger
import           Control.Monad.Reader
import           Control.Monad.Base
import           Data.Conduit
import           Data.Monoid
import qualified Data.Text as T
import           Network.AMQP
import           Network.AMQP.Topic.Source
import           System.Environment
import           System.IO


-- | Toy example of an app stack with MonadLoggerIO.
newtype EgApp a = EgApp { unEgApp:: ReaderT String (LoggingT IO) a
                        } deriving ( Applicative
                                   , Functor
                                   , Monad
                                   , MonadBase IO
                                   , MonadIO
                                   , MonadLogger
                                   , MonadLoggerIO
                                   , MonadReader String
                                   )
-- | Run toy app.
runEgApp:: String
        -> EgApp a
        -> IO a
runEgApp s app =
  runStderrLoggingT (runReaderT (unEgApp app) s)


main :: IO ()
main = do
  as <- getArgs
  (amqpUrl, rKey) <- case as
                       of u : k : [] -> return (u, (T.pack k))
                          _          -> error "Usage: example 'amqp://guest:guest@amqp:5672/' '.something.or.other.#'"

  hSetBuffering stderr LineBuffering

  sourceEvents amqpUrl rKey $$ (printMsg 5) -- logs to stderr without MonadLogger

  sourceEventsL noopEventSourceLogger amqpUrl rKey $$ (printMsg 7) -- runs quietly

  runEgApp "test" $ do x <- ask
                       $(logInfo) $ "Running with " <> (T.pack x)
                       sourceEventsML amqpUrl rKey $$ printMsg 3

  putStrLn "ALL DONE"


printMsg:: (Monad m, MonadIO m)
        => Int -- ^ num messages
        -> Sink (Message,Envelope) m ()
printMsg i = do
    m <- await
    case m of
       Nothing -> printMsg i
       Just (msg,_) -> do liftIO $ putStrLn $ "Received message: " <> ((show.msgBody) msg)
                          if (i>0)
                          then printMsg (i-1)
                          else do liftIO $ putStrLn "THE END!"
                                  return ()
