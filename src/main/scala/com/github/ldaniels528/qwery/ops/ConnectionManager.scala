package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.devices.{InputDevice, OutputDevice}

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
    providers.find { case (name, _) => name.equalsIgnoreCase(serviceName) } map (_._2)
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
trait ConnectionProvider {

  def create(name: String, hints: Option[Hints]): Connection

}
