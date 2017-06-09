package io.scalac.monixamqp

trait TestSetup {

  val ExchangeName = "E1"
  val QueueName = "Q1"
  val RoutingKey = "k"
  
  def mkConnection = RabbitConnection.connect("rabbit1", 5672)

  def setupExchangeAndQueue(): Unit = {
    val connection = mkConnection
    val channel = connection.createChannel()
    channel.exchangeDeclarePassive(ExchangeName)
    channel.queueDeclarePassive(QueueName)
    channel.queueBind(QueueName, ExchangeName, RoutingKey)
    channel.queuePurge(QueueName)
    connection.close()
  }
}
