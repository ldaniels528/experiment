package com.github.ldaniels528.qwery.etl.actors

import akka.actor.{Actor, ActorLogging}
import com.github.ldaniels528.qwery.etl.ETLConfig
import com.github.ldaniels528.qwery.etl.rest.SlaveClient

/**
  * Slave Management Actor
  * @author lawrence.daniels@gmail.com
  */
class SlaveManagementActor(config: ETLConfig) extends Actor with ActorLogging with SlaveClient {
  private implicit val dispatcher = context.dispatcher

  override def baseUrl: String = s"http://${config.workerConfig.supervisor}"

  override def receive: Receive = {
    case request: RegistrationRequest => reflect(registerSlave(request))
    case message =>
      log.warning(s"Unexpected message '$message' (${Option(message).map(_.getClass.getName).orNull})")
      unhandled(message)
  }

}
