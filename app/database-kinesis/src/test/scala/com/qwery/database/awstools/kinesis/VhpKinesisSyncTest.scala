package com.qwery.database.awstools.kinesis

import com.qwery.database.clients.DatabaseClient
import com.qwery.database.awstools.kinesis.KinesisSync.KinesisSyncConfig
import com.qwery.database.server.testkit.DatabaseTestServer
import org.scalatest.funspec.AnyFunSpec

class VhpKinesisSyncTest extends AnyFunSpec {
  private val port = 12118
  private val databaseName = "test"
  private val tableName = "vhp_kinesis_stg"

  // start the server
  DatabaseTestServer.startServer(port)

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
            |) WITH COMMENT 'Messaging client test table'
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

}
