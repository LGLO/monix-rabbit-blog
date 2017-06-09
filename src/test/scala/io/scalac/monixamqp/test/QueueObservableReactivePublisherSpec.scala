package io.scalac.monixamqp.test

import com.rabbitmq.client.{AMQP, Connection}
import io.scalac.monixamqp._
import monix.execution.Scheduler
import org.reactivestreams.Publisher
import org.reactivestreams.tck.{PublisherVerification, TestEnvironment}
import org.scalatest.testng.TestNGSuiteLike
import org.testng.SkipException
import org.testng.annotations.AfterSuite

import scala.concurrent.Future

class QueueObservableReactivePublisherSpec
  extends PublisherVerification[AckableGetResponse](new TestEnvironment())
    with TestNGSuiteLike
    with TestSetup {

  //Lazy val because test framework creates one redundant instance of this spec
  //that is never used, thus `cleanup` is not called.
  lazy val connection: Connection = {
    setupExchangeAndQueue()
    mkConnection
  }
  lazy val channel = connection.createChannel()

  var queuesToCleanup = List.empty[String]

  private def declareQueue(): String = {
    val queue = channel.queueDeclare().getQueue
    queuesToCleanup = queue :: queuesToCleanup
    queue
  }

  @AfterSuite def cleanup() = {
    if (channel.isOpen) {
      queuesToCleanup.foreach(channel.queueDelete)
    }
    connection.abort()
  }

  implicit val s: Scheduler = Scheduler.io()

  // Indicates that QueueObservable never completes, can only signal error.
  // Causes that cases requiring `onComplete` are ignored.
  override val maxElementsFromPublisher = Long.MaxValue

  //Creates publisher emitting `n` elements that will be tested
  override def createPublisher(n: Long) = {
    val queue = declareQueue()
    Future {
      val props = new AMQP.BasicProperties()
      val body = Array.fill(1000)(0.toByte)
      1L.to(n).foreach(_ => channel.basicPublish("", queue, props, body))
    }
    QueueObservable(queue, connection).toReactivePublisher(s)
  }

  //Fails during execution because queue doesn't exist.
  override def createFailedPublisher(): Publisher[AckableGetResponse] =
    QueueObservable("queueThatShouldNotBe", connection).toReactivePublisher(s)

  override def required_spec109_mustIssueOnSubscribeForNonNullSubscriber() =
    throw new SkipException(
      "original spec checks more than written in textual specification"
    )

}