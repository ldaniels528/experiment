package com.qwery.database

import com.qwery.database.StockQuote.randomQuote
import com.qwery.database.wip.MemorySortedPersistentSeq
import com.qwery.util.ServicingTools._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Binary Search Tree Test Suite
 */
class SortedPersistentSeqTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val expectedCount: Int = 1e+6.toInt

  describe("SortedPersistentSeq") {

    it("should sort objects") {
      val (_, processedTime) = time {
        val stocks: Seq[StockQuote] = (1 to expectedCount) map { _ => randomQuote }
        val coll = PersistentSeq.builder[StockQuote].withPartitions(partitionSize = 5000).build
        coll ++= stocks

        val tree = new MemorySortedPersistentSeq[StockQuote, String](_.symbol)
        coll.foreach(tree += _)

        tree.ascending.take(5) foreach (v => logger.info(v.toString))
        logger.info("*" * 20)
        tree.descending.take(5) foreach (v => logger.info(v.toString))
        logger.info("*" * 20)
        logger.info(s"max: ${tree.lastOption}")
        logger.info(s"min: ${tree.headOption}")
        logger.info(s"3rd largest: ${tree.nthLargest(3)}")
        logger.info(s"3rd smallest: ${tree.nthSmallest(3)}")
        logger.info(s"tree.nthLargest(5).exists(tree.contains)? ${tree.nthLargest(5).exists(tree.contains)}")
      }
      logger.info(f"Entire process took $processedTime%.2f msec")
    }
  }

}