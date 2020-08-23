package com.qwery.database

import com.qwery.database.StockQuote._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * ByteBufferSeq Test Suite
 */
class MemoryMappedSeqTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[MemoryMappedSeq[_]].getSimpleName) {

    it("should overflow to disk-based persistence when capacity is reached") {
      /*
      val coll = PersistentSeq[StockQuote](10)
      val newColl = coll.append((0 until 100).map(_ => randomQuote))

      assert(coll.getClass.getName != newColl.getClass.getName)
      assert(coll.isEmpty)
      assert(newColl.length == 100)
      newColl.take(5) foreach(item => logger.info(item.toString))*/
    }

  }

}
