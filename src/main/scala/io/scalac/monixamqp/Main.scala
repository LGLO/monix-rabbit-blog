package io.scalac.monixamqp

import monix.eval.Task
import monix.execution.Ack.{Continue, Stop}
import monix.execution.{Ack, Scheduler}
import monix.reactive.{Consumer, Observable, Observer}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.StdIn

object Main extends App with TestSetup {

  implicit val s: Scheduler = Scheduler.io(name = "rabbit-io")

  //Number of messages to be produced
  val N = 100000

  val zero = 0.toByte
  val body: Array[Byte] = Array.fill(1200 * 100)(zero)

  val msgs = Observable.fromIterable(Stream.from(0).take(N))
    .map(i => OutboundMessage(body, RoutingKey))
    .onCancelTriggerError  
    //.doOnSubscriptionCancel(() => println("subscription cancelled"))
  
  setupExchangeAndQueue()
  val producerConnection = mkConnection
  val consumerConnection = mkConnection

  //val sink = new ExchangeConsumer(producerConnection, ExchangeName)
  val sink = ExchangeConsumer.createSimple(producerConnection, ExchangeName)
  val produce = sink.apply(msgs)
    .doOnFinish(_ => Task(println("produce finished")))
    .runAsync


  val observable = QueueObservable(QueueName, consumerConnection, false)

  val consumeN = observable
    .mapAsync(10)(r => Task(r))
    .bufferTimedAndCounted(5.seconds, 5000)
    .mapAsync(1) { msgs =>
      msgs.lastOption.fold(Task.now(msgs)) { last =>
        last.ack(multiple = true).map(_ => msgs)
      }
    }
    .flatMap(Observable.fromIterable)
    .consumeWith(Consumer.fromObserver(observeN(N/2)))
  
  val consumeSimple = observable
    .mapAsync(1) { m =>
      m.ack().map(_ => m)
    }
    .consumeWith(Consumer.fromObserver(observeN(N/2)))

  //Starting concurrent consumers, note that consumeN fetches more messages than `observeN` consumes
  //thus it can happen that zero, one or both `observeN` completes!
  val cancelable1 = consumeSimple.runAsync
  val cancelable2 = consumeSimple.runAsync
  // consumeN.runAsync.cancel()

  println("Press Enter to cancel producer.")
  StdIn.readLine()
  produce.cancel()
  println("Press Enter to cancel consumers.")
  StdIn.readLine()
  cancelable1.cancel()
  cancelable2.cancel()
  println("Press Enter to close connection.")
  StdIn.readLine()
  producerConnection.close()
  consumerConnection.close()

  private def observeN(n: Int)(s: Scheduler) = {
    new Observer[AckableGetResponse] {

      var consumed = 0

      override def onNext(elem: AckableGetResponse): Future[Ack] = {
        consumed = consumed + 1
        if (consumed == n) {
          println(s"Consumed $n, STOP")
          Stop
        } else {
          Continue
        }
      }

      override def onError(ex: Throwable): Unit = {
        println(s"Observable signals onError($ex)")
        s.reportFailure(ex)
      }

      override def onComplete(): Unit = println("ObserveN onComplete")
    }
  }

}
