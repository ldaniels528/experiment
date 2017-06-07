package com.github.ldaniels528.qwery.ops

import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.devices.{InputDevice, KafkaInputDevice, KafkaOutputDevice, OutputDevice}

import scala.collection.concurrent.TrieMap

/**
  * Connection Manager
  * @author lawrence.daniels@gmail.com
  */
class ConnectionManager {
  private val providers = TrieMap[String, ConnectionProvider]()

  def add(serviceName: String, factory: ConnectionProvider): Unit = {
    providers(serviceName) = factory
  }

  def lookup(serviceName: String): Option[ConnectionProvider] = {
    providers.get(serviceName)
  }
}

/**
  * Connection Manager Singleton
  * @author lawrence.daniels@gmail.com
  */
object ConnectionManager extends ConnectionManager

/**
  * Connection
  * @author lawrence.daniels@gmail.com
  */
trait Connection {

  def name: String

  def getInputDevice(scope: Scope, path: String, hints: Option[Hints]): Option[InputDevice]

  def getOutputDevice(scope: Scope, path: String, hints: Option[Hints]): Option[OutputDevice]

}

/**
  * Connection Factory
  * @author lawrence.daniels@gmail.com
  */
trait ConnectionProvider

/**
  * Kafka Connection Factory
  * @author lawrence.daniels@gmail.com
  */
case class KafkaConnectionProvider(serviceName: String) extends ConnectionProvider {

  def create(name: String): Connection = KafkaConnection(name)

}

/**
  * Kafka Connection
  * @author lawrence.daniels@gmail.com
  */
case class KafkaConnection(name: String) extends Connection {

  override def getInputDevice(scope: Scope, path: String, hints: Option[Hints]): Option[InputDevice] = {
    Option(KafkaInputDevice(topic = path, config = hints.flatMap(_.properties).getOrElse(new JProperties())))
  }

  override def getOutputDevice(scope: Scope, path: String, hints: Option[Hints]): Option[OutputDevice] = {
    Option(KafkaOutputDevice(topic = path, config = hints.flatMap(_.properties).getOrElse(new JProperties())))
  }
}