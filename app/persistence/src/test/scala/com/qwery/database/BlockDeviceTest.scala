package com.qwery.database

import java.nio.ByteBuffer

import com.qwery.database.ColumnTypes.{DoubleType, StringType}
import com.qwery.database.PersistentSeq.{newTempFile, toColumns}
import com.qwery.database.StockQuote.{randomQuote, randomURID}
import com.qwery.util.ServicingTools.time
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.util.Random

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
      val field = eval(f"coll.getField(rowID = 0, columnIndex = 0)", coll.device.getField(rowID = 0, columnIndex = 0))
      assert(field.metadata.`type` == StringType)
    }

    it("should read an individual field value (via column name)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val field = eval(f"coll.getField(rowID = 0, column = 'lastSale)", coll.device.getField(rowID = 0, column = 'lastSale))
      assert(field.metadata.`type` == DoubleType)
    }

    it("should retrieve one row by its offset (rowID)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val rowID = randomURID(coll)
      val row = eval(f"coll.getRow($rowID)", coll.device.getRow(rowID))

      logger.info(s"rowID: \t ${row.rowID}")
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
      val rmd = eval(f"coll.getRowMetaData($rowID)", coll.device.readRowMetaData(rowID))
      logger.info(s"rmd => $rmd")
    }

    it("should retrieve the record size") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val recordSize = eval("coll.recordSize", coll.device.recordSize)
      assert(recordSize == 49)
    }

    it("should remove records by its offset (rowID)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      val rowID = randomURID(coll)
      val before = coll.count()
      eval(f"coll.remove($rowID)", device.remove(rowID))
      val after = coll.count()
      assert(before - after == 1)
    }

    it("should reverse the collection (in place)") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      coll ++= stocks
      eval("coll.reverseInPlace()", coll.device.reverseInPlace())
      assert(coll.toArray sameElements stocks.reverse)
    }

    it("should swap the position of two rows") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      if (coll.nonEmpty) {
        val offset1: ROWID = randomURID(coll)
        val offset2: ROWID = randomURID(coll)
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"BEFORE: $item"))
        eval(s"coll.swap($offset1, $offset2)", coll.device.swap(offset1, offset2))
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"AFTER: $item"))
      }
    }

    it("should shrink the collection by 20%") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      val newSize = (coll.count() * 0.80).toInt
      eval(f"coll.shrinkTo($newSize)", coll.device.shrinkTo(newSize))
      assert(coll.length <= newSize)
    }

    it("should trim dead entries from of the collection") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      eval("(900 to 999).map(coll.remove)", (900 to 999).map(coll.device.remove))
      eval("coll.trim()", coll.device.trim())
    }

    it("should start with an empty collection") {
      val coll = PersistentSeq[StockQuote]()
      implicit val device: BlockDevice = coll.device
      eval("device.truncate()", device.truncate())
      assert(coll.count() == 0)
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

    it("should build an index") {
      val expectedCount: Int = 25
      val (_, processedTime) = time {
        // create a persistent collection
        val stocks: Seq[StockQuote] = (1 to expectedCount) map { _ => randomQuote }
        val coll = PersistentSeq[StockQuote]()
        coll ++= stocks

        // create the index
        val tableDevice = coll.device
        val tableColumns = tableDevice.columns
        val file = newTempFile()
        val indexDevice = tableDevice.createIndex(file, indexColumn = tableColumns(tableColumns.indexWhere(_.name == "symbol")))

        // display the results
        logger.info(s"index.length is ${indexDevice.length}, recordSize is ${indexDevice.recordSize}")
        indexDevice foreach { case (rowID, buf) =>
          showBuffer(rowID, buf)(indexDevice)
        }

        // find a value
        val symbol = stocks(new Random().nextInt(stocks.length)).symbol
        logger.info(s"Looking for '$symbol'...")
        val rowID = indexDevice.search('symbol, Some(symbol))
        logger.info(s"rowID => $rowID")
        assert(rowID.nonEmpty)
      }
      logger.info(f"Entire process took $processedTime%.2f msec")
    }

  }

  def eval[A](label: String, f: => A): A = {
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

  def grabField(rowID: ROWID, columnIndex: Int)(implicit device: BlockDevice): Field = {
    val field@Field(name, fmd, value_?) = device.getField(rowID, columnIndex)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

  def grabField(rowID: ROWID, column: Symbol)(implicit device: BlockDevice): Field = {
    val field@Field(name, fmd, value_?) = device.getField(rowID, column)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

  def showBuffer(rowID: ROWID, buf: ByteBuffer)(implicit indexDevice: BlockDevice): Unit = {
    val row = Map(indexDevice.columns.zipWithIndex flatMap { case (column, idx) =>
      buf.position(indexDevice.columnOffsets(idx))
      val (_, value_?) = Codec.decode(buf)
      value_?.map(value => column.name -> value)
    }: _*)
    logger.info(f"$rowID%d - $row")
  }

}
