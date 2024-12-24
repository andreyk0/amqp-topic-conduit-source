{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}


module Network.AMQP.Topic.Source
  ( AmqpURI
  , ExchangeKey
  , sourceEventsC
  , sourceEventsMv
  ) where


import           Data.Conduit             as C
import qualified Data.Conduit.Combinators as C
import           Network.AMQP
import           RIO
import           UnliftIO.Concurrent
import           UnliftIO.Resource


type ExchangeKey = Text
type AmqpURI = String



{- | Handles reconnections to AMQP server, abstracts an AMQP topic as an infinite
     source of events wrapped into a Conduit.
-}
sourceEventsC
  :: ( Monad m
     , MonadUnliftIO m
     , MonadIO m
     , MonadReader env m
     , MonadResource m
     , HasLogFunc env
     )
  => AmqpURI -- ^ e.g. "amqp://guest:guest@localhost:5672/"
  -> ExchangeKey -- ^ e.g. ".some.topic.#"
  -> ConduitT () (Message,Envelope) m ()
sourceEventsC uri rKey = do
  srcMv <- lift $ sourceEventsMv uri rKey
  C.repeatM $ takeMVar srcMv



{- | Handles reconnections to AMQP server, abstracts an AMQP topic as an infinite
     source of events shared via an MVar.
-}
sourceEventsMv
  :: ( Monad m
     , MonadUnliftIO m
     , MonadIO m
     , MonadReader env m
     , MonadResource m
     , HasLogFunc env
     )
  => AmqpURI -- ^ e.g. "amqp://guest:guest@localhost:5672/"
  -> ExchangeKey -- ^ e.g. ".some.topic.#"
  -> m (MVar (Message,Envelope))
sourceEventsMv uri rKey = do

  sourceMV <- newEmptyMVar
  (_, keepRunningMV) <- allocate (newMVar True) (`putMVar` False)
  lastOpenConnection <- newEmptyMVar

  let ifM c t e = do x <- c; if x then t else e

  let connectToAMQPServer = withRunInIO $ \rio -> do
        rio $ logDebug "Attempting to connect to AMQP host ..."
        connInfo <- fromEither $ first stringException $ fromURI uri
        conn <- liftIO $ openConnection'' connInfo
        putMVar lastOpenConnection conn
        rio $ logDebug "Successfully connected to AMQP host."

        addConnectionClosedHandler conn True $ do
          rio $ logWarn "AMQP connection closed."
          void $ tryPutMVar keepRunningMV True -- try to continue running but don't force it, source may have already closed

        chan <- openChannel conn

        addChannelExceptionHandler chan
          ( \(SomeException e) -> rio $ logError . display $ "AMQP Exception in channel: " <> tshow e )

        -- declare a queue, exchange and binding
        (qName, _, _) <- declareQueue chan newQueue { queueExclusive = True, queueAutoDelete = True }

        rio $ logInfo . display $ "Binding to AMQP queue " <> qName
        bindQueue chan qName "amq.topic" rKey

        rio $ logInfo . display $ "Subscribing to AMQP queue " <> qName

        void $ consumeMsgs chan qName Ack $ \me@(_,env) -> do
          putMVar sourceMV me
          ackEnv env

        rio $ logDebug "Handling AMQP events ..."


  let closeLastOpenConnection = do
        maybeConn <- tryTakeMVar lastOpenConnection
        case maybeConn
          of Just conn -> do logWarn  "Closing AMQP connection ..."
                             catch (liftIO $ closeConnection conn)
                                   (\(SomeException e) -> logError . display $ "Failed to close AMQP connection: " <> tshow e)
             Nothing   -> pure ()
        void $ tryTakeMVar sourceMV -- just in case msg handler callback is blocked there


  let loop = catch ( ifM (takeMVar keepRunningMV)
                            (do closeLastOpenConnection
                                connectToAMQPServer
                                loop)
                            (do closeLastOpenConnection
                                logWarn "Shutting down AMQP event loop ..." ) )

                   (\(SomeException e) -> do
                       logError . display $ "Caught AMQP error: " <> tshow e <> ", sleeping ..."
                       closeLastOpenConnection
                       threadDelay (5 * 1000000)
                       void $ tryPutMVar keepRunningMV True -- try to continue running but don't force it, source may have already closed
                       loop)

  void $ forkIO loop
  pure sourceMV
