package com.github.ldaniels528.qwery.etl.providers

import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.devices.{InputDevice, KafkaInputDevice, KafkaOutputDevice, OutputDevice}
import com.github.ldaniels528.qwery.ops.{Connection, ConnectionProvider, Hints, Scope}

import scala.util.Properties

/**
  * Kafka Connection
  * @author lawrence.daniels@gmail.com
  */
case class KafkaConnection(name: String, hints: Option[Hints]) extends Connection {
  private val consumerProps = hints.flatMap(_.properties) getOrElse new JProperties()

  override def getInputDevice(scope: Scope, path: String, hints: Option[Hints]): Option[InputDevice] = {
    val bootstrapServers = Option(consumerProps.getProperty("bootstrap.servers"))
      .getOrElse(throw new IllegalArgumentException("No bootstrap servers. Set 'bootstrap.servers'"))
    val groupId = Option(consumerProps.getProperty("group.id"))
      .getOrElse(Properties.userName)

    Option(KafkaInputDevice(
      topic = path,
      groupId = groupId,
      bootstrapServers = bootstrapServers,
      consumerProps = hints.flatMap(_.properties)))
  }

  override def getOutputDevice(scope: Scope, path: String, hints: Option[Hints]): Option[OutputDevice] = {
    Option(KafkaOutputDevice(topic = path, config = consumerProps))
  }
}

/**
  * Kafka Connection Factory
  * @author lawrence.daniels@gmail.com
  */
object KafkaConnection extends ConnectionProvider {

  override def create(name: String, hints: Option[Hints]): Connection = KafkaConnection(name, hints)

}