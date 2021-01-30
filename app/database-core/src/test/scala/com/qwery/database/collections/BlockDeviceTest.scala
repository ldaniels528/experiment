package com.qwery.database
package collections

import com.qwery.database.collections.StockQuote._
import com.qwery.database.device.BlockDevice
import com.qwery.database.device.BlockDevice.toColumns
import com.qwery.util.ServicingTools.time
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Block Device Test Suite
 */
class BlockDeviceTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val stocks = Array(
    StockQuote(symbol = "BXXG", exchange = "NASDAQ", lastSale = 147.63, lastSaleTime = 1596317591000L),
    StockQuote(symbol = "KFFQ", exchange = "NYSE", lastSale = 22.92, lastSaleTime = 1597181591000L),
    StockQuote(symbol = "GTKK", exchange = "NASDAQ", lastSale = 240.14, lastSaleTime = 1596835991000L),
    StockQuote(symbol = "KNOW", exchange = "OTCBB", lastSale = 357.21, lastSaleTime = 1597872791000L)
  )
  val (columns, _) = toColumns[StockQuote]

  describe(classOf[BlockDevice].getSimpleName) {

    it("should count the rows where: isCompressed is true") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val count = eval("device.countRows(_.isCompressed)", device.countRows(_.isCompressed))
      assert(count == 0)
    }

    it("should read an individual field value (via column index)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val field = eval(f"device.getField(rowID = 0, columnIndex = 0)", coll.device.getField(rowID = 0, columnID = 0))
      assert(field.value.contains("BXXG"))
    }

    it("should read an individual field value (via column name)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val field = eval(f"device.getField(rowID = 0, column = 'lastSale)", coll.device.getField(rowID = 0, column = 'lastSale))
      assert(field.value.contains(147.63))
    }

    it("should retrieve one row by its offset (rowID)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val rowID = randomURID(coll)
      val row = eval(f"device.getRow($rowID)", coll.device.getRow(rowID))

      logger.info(s"rowID: \t ${row.id}")
      logger.info(s"metadata: \t ${row.metadata}")
      row.fields.zipWithIndex foreach { case (field, index) =>
        logger.info(f"[$index%02d]: \t ${field.name} - ${field.metadata}")
      }
    }

    it("should retrieve one row metadata by its offset (rowID)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val rowID = randomURID(coll)
      val rmd = eval(f"device.getRowMetaData($rowID)", coll.device.readRowMetaData(rowID))
      logger.info(s"rmd => $rmd")
    }

    it("should retrieve the record size") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val recordSize = eval("device.recordSize", coll.device.recordSize)
      assert(recordSize == 41)
    }

    it("should remove records by its offset (rowID)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val rowID = randomURID(coll)
      val before = coll.count()
      eval(f"device.remove($rowID)", device.remove(rowID))
      val after = coll.count()
      assert(before - after == 1)
    }

    it("should reverse the collection (in place)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      eval("device.reverseInPlace()", coll.device.reverseInPlace())
      assert(coll.toArray sameElements stocks.reverse)
    }

    it("should retrieve the row statistics") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      device.remove(0)
      val stats = eval("device.getRowStatistics", device.getRowStatistics)
      assert(stats.deleted == 1)
    }

    it("should swap the position of two rows") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      if (coll.nonEmpty) {
        val offset1: ROWID = randomURID(coll)
        val offset2: ROWID = randomURID(coll)
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"BEFORE: $item"))
        eval(s"device.swap($offset1, $offset2)", coll.device.swap(offset1, offset2))
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"AFTER: $item"))
      }
    }

    it("should shrink the collection by 20%") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      val newSize = (coll.count() * 0.80).toInt
      eval(f"device.shrinkTo($newSize)", coll.device.shrinkTo(newSize))
      assert(coll.length <= newSize)
    }

    it("should trim dead entries from of the collection") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      eval("(900 to 999).map(coll.remove)", (900L to 999L).map(coll.device.remove))
      eval("device.trim()", coll.device.trim())
    }

    it("should read a single field") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      println(s"coll contains ${coll.length} items")
      coll.zipWithIndex.foreach { case (item, index) => println(f"[${index + 1}] $item") }
      println()

      // read the symbol from the 1st record
      assert(grabField(rowID = 0, columnIndex = 0).value.contains("BXXG"))

      // read the exchange from the 2nd record
      assert(grabField(rowID = 1, column = 'exchange).value.contains("NYSE"))

      // read the lastSale from the 3rd record
      assert(grabField(rowID = 2, column = 'lastSale).value.contains(240.14))

      // read the lastSaleTime from the 4th record
      assert(grabField(rowID = 3, columnIndex = 3).value.contains(1597872791000L))
    }

  }

  private def eval[A](label: String, f: => A): A = {
    val (results, runTime) = time(f)
    val output = results match {
      case items: PersistentSeq[_] => f"(${items.length} items)"
      case value: Double => f"${value.toDouble}%.2f"
      case items: Seq[_] => f"(${items.length} items)"
      case it: Iterator[_] => if (it.hasNext) s"<${it.next()}, ...>" else "<empty>"
      case x => x.toString
    }
    logger.info(f"$label ~> $output [$runTime%.2f msec]")
    results
  }

  private def grabField(rowID: ROWID, columnIndex: Int)(implicit device: BlockDevice): Field = {
    val field@Field(name, fmd, value_?) = device.getField(rowID, columnIndex)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

  private def grabField(rowID: ROWID, column: Symbol)(implicit device: BlockDevice): Field = {
    val field@Field(name, fmd, value_?) = device.getField(rowID, column)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

}
