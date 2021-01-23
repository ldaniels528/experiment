package com.qwery.database
package collections

import com.qwery.database.collections.StockQuote.randomQuote
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Partitioned Persistent Sequence Test Suite
 */
class PartitionedPersistentSeqTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val quotes2 = (0 to 1).map(_ => randomQuote)
  private val quotes10 = (0 to 10).map(_ => randomQuote)

  describe(classOf[PersistentSeq[StockQuote]].getSimpleName) {

    it("should read/write data into a single partition") {
      val coll = PersistentSeq.builder[StockQuote].withPartitions(partitionSize = 3).build
      coll ++= quotes2
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index+1}] $q") }
      assert(coll.length == 2)
    }

    it("should read/write data into a single partition on the edge of a second partition") {
      val coll = PersistentSeq.builder[StockQuote].withPartitions(partitionSize = 2).build
      coll ++= quotes2
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index+1}] $q") }
      assert(coll.length == 2)
    }

    it("should read/write data into two partitions") {
      val coll = PersistentSeq.builder[StockQuote].withPartitions(partitionSize = 1).build
      coll ++= quotes2
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index+1}] $q") }
      assert(coll.length == 2)
    }

    it("should read/write data into multiple partitions") {
      val coll = PersistentSeq.builder[StockQuote].withPartitions(partitionSize = 2).build
      coll ++= quotes10
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index+1}] $q") }
      assert(coll.length == 11)
    }

  }

}