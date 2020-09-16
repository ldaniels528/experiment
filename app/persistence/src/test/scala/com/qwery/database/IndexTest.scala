package com.qwery.database

import com.qwery.database.PersistentSeq.newTempFile
import com.qwery.database.StockQuote.randomQuote
import com.qwery.util.ServicingTools._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class IndexTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val expectedCount: Int = 1e+2.toInt

  describe("Index") {

    it("should build an index") {
      val (_, processedTime) = time {
        val stocks: Seq[StockQuote] = (1 to expectedCount) map { _ => randomQuote }
        val coll = PersistentSeq.builder[StockQuote].withPartitions(partitionSize = 5000).build
        coll ++= stocks

        val tableDevice = coll.device
        val tableColumns = tableDevice.columns
        val file = newTempFile()
        val indexDevice = tableDevice.createIndex(file, indexColumn = tableColumns(tableColumns.indexWhere(_.name == "symbol")))

        logger.info(s"index.length is ${indexDevice.length}, recordSize is ${indexDevice.recordSize}")
        indexDevice foreach { case (rowID, buf) =>
          val values = indexDevice.columns.zipWithIndex map { case (_, idx) =>
            buf.position(indexDevice.columnOffsets(idx))
            Codec.decode(buf)
          }
          logger.info(f"$rowID%d - ${values}")
        }
      }
      logger.info(f"Entire process took $processedTime%.2f msec")
    }

  }

}
