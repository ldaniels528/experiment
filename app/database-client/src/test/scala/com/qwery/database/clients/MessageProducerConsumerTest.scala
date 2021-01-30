package com.qwery.database.clients

import com.qwery.database.server.testkit.DatabaseTestServer
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Messaging Producer-Consumer Test Suite
 * @author lawrence.daniels@gmail.com
 */
class MessageProducerConsumerTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val port = 12117
  private val databaseName = "test"
  private val tableName = "messaging_client_test"

  // start the server
  DatabaseTestServer.startServer(port)

  // create the clients
  private val databaseClient = DatabaseClient(port = port)
  private val messageProducer = MessageProducer(port = port)
  private val messageConsumer = MessageConsumer(port = port, databaseName = databaseName, tableName = tableName)

  describe(classOf[DatabaseClient].getSimpleName) {

    it("should create a new table on the server") {
      // drop the table
      databaseClient.dropTable(databaseName, tableName, ifExists = true)

      // create the table
      databaseClient.executeQuery(databaseName,
        s"""|CREATE TABLE IF NOT EXISTS $tableName (
            |  symbol STRING(8) comment 'the ticker symbol',
            |  exchange STRING(8) comment 'the stock exchange',
            |  lastSale DOUBLE comment 'the latest sale price',
            |  lastSaleTime LONG comment 'the latest sale date/time'
            |) WITH DESCRIPTION 'Messaging client test table'
            |""".stripMargin
      )
    }
  }

  describe(classOf[MessageProducer].getSimpleName) {

    it("should append messages to a table on the server") {
      val message =  s"""{"symbol":"AAPL", "exchange":"NYSE", "lastSale":900, "lastSaleTime":1611772605427}"""
      logger.info(s"message: $message")

      val response = messageProducer.send(databaseName, tableName, message)
      logger.info(s"response = $response")
      assert(response.count == 1)
    }
  }

  describe(classOf[DatabaseClient].getSimpleName) {

    it("should query messages from the server") {
      val results = databaseClient.executeQuery(databaseName, s"SELECT * FROM $tableName")
      results.foreachKVP { row =>
        logger.info(s"row: $row")
        assert(row.toMap == Map("exchange" -> "NYSE", "symbol" -> "AAPL", "lastSale" -> 900.0, "lastSaleTime" -> 1611772605427L))
      }
      assert(results.rows.size == 1)
    }
  }

  describe(classOf[MessageConsumer].getSimpleName) {

    it("should retrieve messages (if available) from the server") {
      val message = messageConsumer.getNextMessage
      logger.info(s"message: $message")
      assert(message.map(_.toMap).contains(Map("__id" -> 0, "exchange" -> "NYSE", "symbol" -> "AAPL", "lastSale" -> 900.0, "lastSaleTime" -> 1611772605427L)))
    }
  }

}