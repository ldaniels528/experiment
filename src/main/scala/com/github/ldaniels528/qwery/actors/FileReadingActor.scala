package com.github.ldaniels528.qwery.actors

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.github.ldaniels528.qwery.actors.FileReadingActor.{DataReceived, EOF, ReadFile}
import com.github.ldaniels528.qwery.ops.{RootScope, Row}
import com.github.ldaniels528.qwery.sources.DataResource
import com.github.ldaniels528.qwery.util.ResourceHelper._

/**
  * File Reading Actor
  * @author lawrence.daniels@gmail.com
  */
class FileReadingActor() extends Actor with ActorLogging {
  override def receive: Receive = {
    case ReadFile(pid, resource, recipient) =>
      log.info(s"$pid: Reading file '$resource'")
      resource.getInputSource match {
        case Some(source) =>
          source.open(RootScope())
          source use { device =>
            var record: Option[Row] = None
            do {
              record = device.read()
              record.foreach(recipient ! DataReceived(pid, _))
            } while (record.nonEmpty)
          }
          recipient ! EOF(pid, resource)
        case None =>
          log.error(s"No device could be determined for '$resource'")
      }

    case message =>
      unhandled(message)
  }
}

/**
  * File Reading Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object FileReadingActor {

  case class ReadFile(pid: UUID, resource: DataResource, recipient: ActorRef)

  case class DataReceived(pid: UUID, row: Row)

  case class EOF(pid: UUID, resource: DataResource)

}