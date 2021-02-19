package com.qwery.database
package server
package testkit

import akka.actor.ActorSystem
import org.slf4j.LoggerFactory

/**
 * Database Test Server
 */
object DatabaseTestServer {
  private val logger = LoggerFactory.getLogger(getClass)

  def startServer(port: Int): Unit = {
    implicit val system: ActorSystem = ActorSystem(name = "test-server")
    import system.dispatcher

    logger.info(s"Starting Test Database Server on port $port...")
    DatabaseServer.startServer(port = port)
  }

}
