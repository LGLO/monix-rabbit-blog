package io.scalac.monixamqp

import com.rabbitmq.client._

/** Keeping it trivial for blog-size reasons */
case class ConnectionSettings(host: String, port: Int)

object RabbitConnection {

  def connect(host: String, port: Int): Connection = {
    val cf = new ConnectionFactory()
    cf.setHost(host)
    cf.setPort(port)
    cf.newConnection()
  }
}
