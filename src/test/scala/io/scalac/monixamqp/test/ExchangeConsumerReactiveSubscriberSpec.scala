package io.scalac.monixamqp.test

import io.scalac.monixamqp.{ExchangeConsumer, OutboundMessage, TestSetup}
import monix.eval.Callback
import monix.execution.Scheduler
import org.reactivestreams.tck.{SubscriberBlackboxVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike
import org.testng.annotations.AfterSuite

class ExchangeConsumerReactiveSubscriberSpec
  extends SubscriberBlackboxVerification[OutboundMessage](
    new TestEnvironment(300)
  ) with TestNGSuiteLike
    with TestSetup {

  /* 
   You have to provide RabbitServer manually,
   there is no code in this project that does it.
   Lazy val because test framework creates on redundant instance
   that never calls `cleanup`.
   */
  lazy val connection = {
    setupExchangeAndQueue()
    mkConnection
  }

  @AfterSuite def cleanup() = connection.close()

  implicit val scheduler = Scheduler.io()

  override def createSubscriber() =
    new ExchangeConsumer(connection, ExchangeName)
      .createSubscriber(Callback.empty, scheduler)
      ._1.toReactive(1)

  override def createElement(element: Int) =
    OutboundMessage(routingKey = "foo", body = BigInt(element).toByteArray)
}
