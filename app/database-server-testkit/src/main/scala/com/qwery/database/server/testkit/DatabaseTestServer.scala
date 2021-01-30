package com.qwery.database.server.testkit

import akka.actor.ActorSystem
import akka.util.Timeout
import com.qwery.database.server.{DatabaseServer, QueryProcessor}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

/**
 * Database Test Server
 */
object DatabaseTestServer {
  private val logger = LoggerFactory.getLogger(getClass)

  def startServer(port: Int, timeout: Timeout = 2.minutes): Unit = {
    implicit val system: ActorSystem = ActorSystem(name = "test-server")
    implicit val _timeout: Timeout = timeout
    implicit val queryProcessor: QueryProcessor = new QueryProcessor()
    import system.dispatcher

    logger.info(s"Starting Test Database Server on port $port...")
    DatabaseServer.startServer(port = port)
  }

}
