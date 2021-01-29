package com.qwery.database.kinesis

import akka.actor.ActorSystem
import akka.util.Timeout
import com.qwery.database.clients.DatabaseClient
import com.qwery.database.kinesis.KinesisSync.KinesisSyncConfig
import com.qwery.database.server.{DatabaseServer, QueryProcessor}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.concurrent.duration._

class KinesisSyncTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val port = 12118
  private val databaseName = "test"
  private val tableName = "vhp_kinesis_stg"

  // start the server
  startServer(port)

  // create the clients
  private val databaseClient = DatabaseClient(port = port)

  describe(classOf[DatabaseClient].getSimpleName) {

    it("should create a new table on the server") {
      // drop the table
      //databaseClient.dropTable(databaseName, tableName, ifExists = true)

      // create the table
      databaseClient.executeQuery(databaseName,
        s"""|CREATE TABLE IF NOT EXISTS $tableName (
            |  visitorId STRING(128) comment 'the visitor ID',
            |  dealerCode STRING(128) comment 'the dealer code',
            |  pageType STRING(128) comment 'the page type',
            |  year STRING(4) comment 'the year (e.g. 2021)',
            |  t0 STRING(20) comment 'the t0 - EPOC/millis'
            |) WITH DESCRIPTION 'Messaging client test table'
            |""".stripMargin
      )
    }
  }

  describe(KinesisSync.getClass.getSimpleName) {

    it("should persist Kinesis messages") {
      KinesisSync.start(KinesisSyncConfig(
        host = "0.0.0.0",
        port = port,
        databaseName = databaseName,
        tableName = tableName,
        applicationName = "test-tools-cbhp-kinesis-stg",
        streamName = "vhp-kinesis-stg",
        region = "us-east-1"
      ))
    }

  }

  def startServer(port: Int): Unit = {
    implicit val system: ActorSystem = ActorSystem(name = "test-server")
    implicit val timeout: Timeout = 2.minutes
    implicit val queryProcessor: QueryProcessor = new QueryProcessor()
    import system.dispatcher

    logger.info(s"Starting Database Server on port $port...")
    DatabaseServer.startServer(port = port)
  }

}
