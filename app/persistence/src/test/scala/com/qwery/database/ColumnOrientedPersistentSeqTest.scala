package com.qwery.database

import com.qwery.database.StockQuote.randomQuote
import com.qwery.database.device.ColumnOrientedBlockDevice
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Column-Oriented Persistent Sequence Test Suite
 */
class ColumnOrientedPersistentSeqTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val quotes10 = (1 to 10).map(_ => randomQuote)

  describe(classOf[ColumnOrientedBlockDevice].getSimpleName) {
    val coll = PersistentSeq.builder[StockQuote]
      .withTable(databaseName = "test", tableName = "columnar")
      .withColumnModel
      .build

    it("should write data into columnar files") {
      coll.device.shrinkTo(0)
      coll ++= quotes10
      assert(coll.length == 10)
    }

    it("should read data from columnar files") {
      val items = quotes10.indices.map(coll.apply)
      items.foreach { q => logger.info(q.toString) }
    }

  }

}