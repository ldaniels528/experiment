package com.github.ldaniels528.qwery.etl.actors

import akka.actor.{Actor, ActorLogging}

/**
  * Kafka Message Trigger Actor
  * @author lawrence.daniels@gmail.com
  */
class KafkaMessageTriggerActor extends Actor with ActorLogging {

  override def receive: Receive = {
    case message =>
      unhandled(message)
  }

}
