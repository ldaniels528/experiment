package com.qwery.database

import com.qwery.database.StockQuote.randomQuote
import com.qwery.database.server.JSONSupport.JSONProductConversion
import com.qwery.database.server.ServerSideTableService
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * PersistentSeq Database Test Suite
 */
class PersistentSeqDatabaseTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val service: ServerSideTableService = ServerSideTableService()

  describe(classOf[PersistentSeq[_]].getSimpleName) {
    val databaseName = "test"
    val tableName = "stockQuotes"

    it("should read/write local database tables") {
      // drop the previous table (if it exists)
      service.executeQuery(databaseName, sql = s"DROP TABLE $tableName")

      // create the table
      service.executeQuery(databaseName, sql =
        s"""|CREATE TABLE $tableName (
            |  symbol STRING(8) comment 'the ticker symbol',
            |  exchange STRING(8) comment 'the stock exchange',
            |  lastSale DOUBLE comment 'the latest sale price',
            |  lastSaleTime LONG comment 'the latest sale date/time'
            |)
            |LOCATION '/$databaseName/$tableName/'
            |""".stripMargin
      )

      // reference the table with our collection
      val stocks = PersistentSeq[StockQuote](databaseName, tableName)

      // insert 20 records
      stocks ++= (1 to 20).map(_ => randomQuote)

      // display the records
      stocks.foreach(stock => logger.info(stock.toJSON))
      assert(stocks.count() == 20)
    }

  }

}
