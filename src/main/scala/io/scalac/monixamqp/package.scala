package io.scalac

import com.rabbitmq.client.{Channel, GetResponse}
import monix.eval.Task

package object monixamqp {

  /** Simplistic for demo reasons */
  case class OutboundMessage(body: Array[Byte], routingKey: String)

  class AckableGetResponse(val response: GetResponse, channel: Channel) {
    def ack(multiple: Boolean = false): Task[Unit] = Task {
      channel.synchronized(channel.basicAck(response.getEnvelope.getDeliveryTag, multiple))
    }
  }

}
