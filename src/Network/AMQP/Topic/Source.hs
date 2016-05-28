{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}


module Network.AMQP.Topic.Source (
  AmqpURI
, EventSourceLogger(..)
, ExchangeKey
, noopEventSourceLogger
, sourceEvents
, sourceEventsL
, sourceEventsML
, stderrEventSourceLogger
) where


import           Control.Concurrent
import qualified Control.Exception as CE
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Control.Monad.Logger
import           Control.Monad.Trans.Class
import           Data.Conduit
import           Data.Monoid
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import           Language.Haskell.TH.Syntax (qLocation)
import           Network.AMQP
import           System.IO (stderr)


type ExchangeKey = Text
type AmqpURI = String

-- | Log in the IO monad.
data EventSourceLogger =
  EventSourceLogger { esLog:: LogLevel -> Text -> IO ()
                    }

-- | Don't log anything
noopEventSourceLogger:: EventSourceLogger
noopEventSourceLogger =
  EventSourceLogger $ \_ _ -> return ()

-- | Log to stderr
stderrEventSourceLogger:: EventSourceLogger
stderrEventSourceLogger =
  EventSourceLogger $ \lvl msg ->
    TIO.hPutStrLn stderr $ (T.pack.show) lvl <> ": " <> msg


-- | Handles reconnections to AMQP server, abstracts an AMQP topic as an infinite
--   source of events.
-- Logs to stderr.
sourceEvents:: (Monad m, MonadIO m)
            => AmqpURI -- ^ e.g. "amqp://guest:guest@localhost:5672/"
            -> ExchangeKey -- ^ e.g. ".some.topic.#"
            -> Source m (Message,Envelope)
sourceEvents = sourceEventsL stderrEventSourceLogger


-- | Handles reconnections to AMQP server, abstracts an AMQP topic as an infinite
--   source of events.
-- Logs with MonadLogger.
sourceEventsML:: (Monad m, MonadIO m, MonadLoggerIO m)
              => AmqpURI -- ^ e.g. "amqp://guest:guest@localhost:5672/"
              -> ExchangeKey -- ^ e.g. ".some.topic.#"
              -> Source m (Message,Envelope)
sourceEventsML uri rKey = do
  mllg <- lift askLoggerIO
  let lg = EventSourceLogger $ \lvl msg -> mllg $(qLocation >>= liftLoc) "sourceEventsML" lvl (toLogStr msg)
  sourceEventsL lg uri rKey


-- | Handles reconnections to AMQP server, abstracts an AMQP topic as an infinite
--   source of events.
-- Takes logger implementation.
sourceEventsL:: (Monad m, MonadIO m)
             => EventSourceLogger
             -> AmqpURI -- ^ e.g. "amqp://guest:guest@localhost:5672/"
             -> ExchangeKey -- ^ e.g. ".some.topic.#"
             -> Source m (Message,Envelope)
sourceEventsL lg uri rKey = do

  sourceMV <- liftIO $ newEmptyMVar
  keepRunningMV <- liftIO $ newMVar True
  lastOpenConnection <- liftIO $ newEmptyMVar

  let ifM c t e = do x <- c; if x then t else e

  let connectToAMQPServer = do
        (esLog lg LevelDebug) "Attempting to connect to AMQP host ..."
        conn <- openConnection'' $ fromURI uri
        putMVar lastOpenConnection conn
        (esLog lg LevelDebug) "Successfully connected to AMQP host."

        addConnectionClosedHandler conn True $ do
          (esLog lg LevelWarn) "AMQP connection closed."
          _ <- tryPutMVar keepRunningMV True -- try to continue running but don't force it, source may have already closed
          return ()

        chan <- openChannel conn

        addChannelExceptionHandler chan
          ( \(e::CE.SomeException) ->
               (esLog lg LevelError) $ "AMQP Exception in channel: " <> (T.pack.show) e )

        -- declare a queue, exchange and binding
        (qName, _, _) <- declareQueue chan newQueue { queueExclusive = True, queueAutoDelete = True }

        (esLog lg LevelInfo) $ "Binding to AMQP queue " <> qName
        bindQueue chan qName "amq.topic" rKey

        (esLog lg LevelInfo) $ "Subscribing to AMQP queue " <> qName

        _ <- consumeMsgs chan qName Ack $ \me@(_,env) -> do
               putMVar sourceMV me
               ackEnv env

        (esLog lg LevelDebug) "Handling AMQP events ..."


  let closeLastOpenConnection = do maybeConn <- tryTakeMVar lastOpenConnection
                                   case maybeConn
                                     of Just conn -> do (esLog lg LevelWarn) "Closing AMQP connection ..."
                                                        closeConnection conn
                                        Nothing   -> return ()
                                   _ <- liftIO $ tryTakeMVar sourceMV -- just in case msg handler callback is blocked there
                                   return ()


  let loop = CE.catch ( ifM (takeMVar keepRunningMV)
                            (do closeLastOpenConnection
                                connectToAMQPServer
                                loop)
                            (do closeLastOpenConnection
                                (esLog lg LevelWarn) "Shutting down AMQP event loop ..." ) )

                      (\(e::CE.SomeException) -> do (esLog lg LevelError) $ "Caught AMQP error: " <> ((T.pack.show) e) <> ", sleeping ..."
                                                    closeLastOpenConnection
                                                    threadDelay (5 * 1000000)
                                                    _ <- tryPutMVar keepRunningMV True -- try to continue running but don't force it, source may have already closed
                                                    loop)

  _ <- liftIO $ forkIO loop


  let cSource = do nextMsg <- liftIO $ takeMVar sourceMV
                   yieldOr nextMsg (liftIO $ putMVar keepRunningMV False)
                   cSource

  cSource
