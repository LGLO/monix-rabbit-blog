package io.scalac.monixamqp

import com.rabbitmq.client.{Channel, Connection}
import monix.eval.Task
import monix.execution.Ack.{Continue, Stop}
import monix.execution.Scheduler
import monix.reactive.Observable

import scala.concurrent.blocking
import scala.util.control.NonFatal

object QueueObservable {

  /**
    * Observes one RabbitMQ `queue`. Infinite by design.
    * Each message will be delivered to only one of `Subscribers`.
    *
    * @param queue      - name of queue from which messages are consumed.
    * @param connection - used to obtain private `Channel` per `Subscriber`.
    * @param autoAck    - messages can be autoAck when delivered to this
    *                   Observable. It is consumer responsibility to ack not
    *                   `autoAck`-ed message and to not ack twice.
    */
  def apply(
    queue: String,
    connection: Connection,
    autoAck: Boolean = true
  ): Observable[AckableGetResponse] =
    Observable.unsafeCreate { subscriber =>

      implicit val s: Scheduler = subscriber.scheduler

      //Flag used to cancel feeding
      var continue = true
      def cancelFeeding(): Unit = continue = false
      /**
        * Trampolines itself or returns unit Task to stop process
        * or returns failed Task to signal error.
        *
        * @param ch `subscriber`s private channel to get messages
        * @return Task that feeds `subscriber` with next message read from
        *         `channel`
        */
      def feedSubscriber(ch: Channel): Task[Unit] = {
        def abort: Task[Unit] =
          Task {
            if (ch.isOpen) ch.abort()
          }

        def oneGet: Task[Unit] =
          try blocking(ch.basicGet(queue, autoAck)) match {
            // No messages waiting for consumption
            case null =>
              //Simple busy spinning. It's not Aeron, I waste resources here.
              feedSubscriber(ch)
            case resp =>
              val ackableResp = new AckableGetResponse(resp, ch)
              Task.fromFuture(subscriber.onNext(ackableResp))
                .flatMap {
                  case Continue =>
                    feedSubscriber(ch)
                  case Stop =>
                    abort
                }
          } catch {
            case NonFatal(ex) =>
              abort.flatMap(_ => Task.raiseError(ex))
          }

        Task.defer {
          if (continue) oneGet else abort
        }
      }

      val feeding =
        Task(connection.createChannel()).flatMap { ch =>
          feedSubscriber(ch)
            .doOnCancel(Task(cancelFeeding()))
        }.onErrorRecover {
          case NonFatal(ex) => subscriber.onError(ex)
        }

      feeding.runAsync
    }
}
