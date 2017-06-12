package io.scalac.monixamqp

import com.rabbitmq.client.{AMQP, Connection}
import monix.eval.Callback
import monix.execution.Ack.Continue
import monix.execution.cancelables.AssignableCancelable
import monix.execution.{Ack, Scheduler}
import monix.reactive.observers.Subscriber
import monix.reactive.{Consumer, Observer}

import scala.concurrent.{Future, blocking}

/**
  * Simple Monix consumer for sending to an Exchange without Publisher Confirms
  * and without Channel recovery.
  *
  * @param connection - Connection used to create Subscribers private channel
  * @param exchange   - exchange name
  */
class ExchangeConsumer(connection: Connection, exchange: String)
  extends Consumer[OutboundMessage, Unit] {

  override def createSubscriber(
    cb: Callback[Unit],
    s: Scheduler
  ): (Subscriber[OutboundMessage], AssignableCancelable) = {
    val subscriber =
      new Subscriber[OutboundMessage] {
        private val ch = connection.createChannel()
        private val properties = new AMQP.BasicProperties()
        implicit val scheduler: Scheduler = s

        private def publish(m: OutboundMessage): Unit =
          ch.basicPublish(exchange, m.routingKey, properties, m.body)

        override def onNext(m: OutboundMessage): Future[Ack] =
          Future {
            blocking(publish(m))
            Continue
          }

        override def onError(ex: Throwable): Unit = {
          abort()
          cb.onError(ex)
        }

        override def onComplete(): Unit = {
          abort()
          cb.onSuccess(())
        }

        private def abort(): Unit = if (ch.isOpen) ch.abort()
      }
    val cancelable = AssignableCancelable.single()
    connection.addShutdownListener(_ => cancelable.cancel())
    (subscriber, cancelable)
  }
}

object ExchangeConsumer {

  /**
    * The simplest way for creating Consumer[OutboundMessage, Unit].
    * Downside is that it leaves channel open when canceled.
    */
  def createSimple(
    connection: Connection,
    exchange: String
  ): Consumer[OutboundMessage, Unit] =
    Consumer.fromObserver { s =>
      new Observer[OutboundMessage] {
        val ch = connection.createChannel()
        val properties = new AMQP.BasicProperties()

        def publish(m: OutboundMessage) =
          ch.basicPublish(exchange, m.routingKey, properties, m.body)

        def onNext(m: OutboundMessage): Future[Ack] =
          Future {
            blocking(publish(m))
            Continue
          }(s)

        def onError(ex: Throwable): Unit = abort()

        def onComplete(): Unit = abort()

        private def abort(): Unit = if (ch.isOpen) ch.abort()
      }
    }

  /**
    * Uses Consumer.create convenience method, that injects cancelable,
    * which will cancel data producer.
    */
  def createCancelable(
    connection: Connection,
    exchange: String
  ): Consumer[OutboundMessage, Unit] =
    Consumer.create { (s, cancelable, cb) =>
      new Observer[OutboundMessage] {
        val ch = connection.createChannel()
        val properties = new AMQP.BasicProperties()

        connection.addShutdownListener(_ => cancelable.cancel())

        def publish(m: OutboundMessage) =
          ch.basicPublish(exchange, m.routingKey, properties, m.body)

        def onNext(m: OutboundMessage): Future[Ack] =
          Future {
            blocking(publish(m))
            Continue
          }(s)

        override def onError(ex: Throwable): Unit = {
          abort()
          cb.onError(ex)
        }

        override def onComplete(): Unit = {
          abort()
          cb.onSuccess(())
        }

        //Checks if is open to mitigate risk of killing connection
        private def abort(): Unit = if (ch.isOpen) ch.abort()
      }
    }
}