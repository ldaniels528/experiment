package com.qwery.database

import com.qwery.database.PersistentSeq.newTempFile
import com.qwery.database.StockQuote._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class HybridPersistentSeqTest extends AnyFunSpec {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val quotes2 = (0 to 1).map(_ => randomQuote)
  private val quotes4 = (0 to 3).map(_ => randomQuote)

  describe(classOf[PersistentSeq[StockQuote]].getSimpleName) {
    
    it("should read/write data from/to memory area") {
      val coll = PersistentSeq.builder[StockQuote]
        .withMemoryCapacity(2)
        .withPersistenceFile(newTempFile())
        .build
      coll ++= quotes2
      coll.foreach(q => logger.info(s"mem: $q"))
    }

    it("should read/write data from/to disk area") {
      val coll = PersistentSeq.builder[StockQuote]
        .withMemoryCapacity(0)
        .withPersistenceFile(newTempFile())
        .build
      coll ++= quotes2
      coll.foreach(q => logger.info(s"disk: $q"))
    }

    it("should read/write data across memory and disk areas") {
      val coll = PersistentSeq.builder[StockQuote]
        .withMemoryCapacity(2)
        .withPersistenceFile(newTempFile())
        .build
      coll ++= quotes4
      coll.foreach(q => logger.info(q.toString))
    }

  }

}
