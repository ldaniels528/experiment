package com.qwery.database

import java.io.File

import com.qwery.database.StockQuote.randomQuote
import com.qwery.util.ServicingTools._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Binary Search Tree Test Suite
 */
class SortedPersistentSeqTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val expectedCount: Int = 1e+6.toInt

  describe("BSTree") {

    it("should sort objects") {
      val tree = new DiskMappedSortedPersistentSeq[StockQuote, String](new File("./temp.bin"))(_.symbol)
      val _tree = new MemorySortedPersistentSeq[StockQuote, String](_.symbol)
      val (_, sortTime) = time(for (_ <- 1 to expectedCount) tree.add(randomQuote))
      logger.info(f"Sorted $expectedCount items in $sortTime%.2f")

      tree.ascending.take(5) foreach (v => println(v))
      println("*" * 20)
      tree.descending.take(5) foreach (v => println(v))
      println("*" * 20)
      println(s"max: ${tree.lastOption}")
      println(s"min: ${tree.headOption}")
      println(s"3rd largest: ${tree.nthLargest(3)}")
      println(s"3rd smallest: ${tree.nthSmallest(3)}")
      println(s"tree.nthLargest(5).exists(tree.contains)? ${tree.nthLargest(5).exists(tree.contains)}")
      //println(s"tree.contains(-1)? ${tree.contains(-1)}")
    }
  }

}
