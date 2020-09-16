package com.qwery.database

import java.io.File

import com.qwery.database.ColumnTypes.{DoubleType, IntType, LongType, StringType}
import com.qwery.database.PersistentSeq.Field
import com.qwery.database.StockQuote._
import com.qwery.util.ServicingTools._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.Random

/**
 * Persistent Sequence Test Suite
 */
class PersistentSeqTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val expectedCount: Int = 1e+5.toInt

  def newCollection[A <: Product : ClassTag](partitionSize: Int): PersistentSeq[A] = {
    PersistentSeq.builder[A]
      //.withMemoryCapacity(capacity = (expectedCount * 1.2).toInt)
      .withParallelism(ExecutionContext.global)
      .withPartitions(partitionSize = partitionSize)
      .build
  }

  private val coll = newCollection[StockQuote](partitionSize = expectedCount / 25)
  private val stockList: Seq[StockQuote] = (1 to expectedCount) map { _ => randomQuote }
  private val bonjourStock = randomQuote.copy(symbol = "BELLE")
  private val stocks4 = Array(
    StockQuote(symbol = "BXXG", exchange = "NASDAQ", lastSale = 147.63, lastSaleTime = 1596317591000L),
    StockQuote(symbol = "KFFQ", exchange = "NYSE", lastSale = 22.92, lastSaleTime = 1597181591000L),
    StockQuote(symbol = "GTKK", exchange = "NASDAQ", lastSale = 240.14, lastSaleTime = 1596835991000L),
    StockQuote(symbol = "KNOW", exchange = "OTCBB", lastSale = 357.21, lastSaleTime = 1597872791000L)
  )

  describe(classOf[PersistentSeq[_]].getSimpleName) {
    logger.info(s"coll is a ${coll.getClass.getSimpleName}(${coll.device.getClass.getSimpleName}); sample size is $expectedCount")

    it("should extract values from a product class") {
      val ps = PersistentSeq[GenericData]()
      val data = GenericData(idValue = "Hello", idType = "World", responseTime = 307, reportDate = 1592204400000L, _id = 1087L)
      val values = ps.toKeyValues(data)
      assert(values == Seq(
        "idValue" -> Some("Hello"),
        "idType" -> Some("World"),
        "responseTime" -> Some(307),
        "reportDate" -> Some(1592204400000L),
        "_id" -> Some(1087L)
      ))
    }

    it("should populate a product class with vales") {
      val ps = PersistentSeq[GenericData]()
      val fmd = FieldMetaData(isCompressed = false, isEncrypted = false, isNotNull = true, `type` = StringType)
      val data = ps.createItem(Seq(
        Field(name = "_id", fmd.copy(`type` = LongType), value = Some(1087L)),
        Field(name = "idValue", fmd, Some("Hello")),
        Field(name = "idType", fmd, Some("World")),
        Field(name = "responseTime", fmd.copy(`type` = IntType), Some(307)),
        Field(name = "reportDate", fmd.copy(`type` = LongType), Some(java.sql.Date.valueOf("2020-06-15").getTime))
      ))
      assert(data == GenericData(idValue = "Hello", idType = "World", responseTime = 307, reportDate = 1592204400000L, _id = 1087))
    }

    it("should start with an empty collection") {
      eval("coll.truncate()", coll.device.truncate())
      assert(coll.count() == 0)
    }

    it("should append a collection of items to the collection") {
     val items = eval("coll.append(Seq(..))", coll.append(stockList))
      assert(items.count() == expectedCount)
    }

    it("should append an item to the collection") {
      val items = eval(s"coll.append($bonjourStock)", coll.append(bonjourStock))
      assert(items.count() == 1 + expectedCount)
    }

    it("should retrieve one record with row metadata by its offset (rowID)") {
      val rowID = randomURID(coll)
      eval(f"coll.apply($rowID)", coll(rowID))
    }

    it("should compute avg(lastSale)") {
      eval("coll.avg(_.lastSale)", coll.avg(_.lastSale))
    }

    it("should count all items") {
      val total = eval("coll.count()", coll.count())
      assert(total == expectedCount + 1)
    }

    it("should count the items where: lastSale <= 500") {
      eval("coll.count(_.lastSale <= 500))", coll.count(_.lastSale <= 500))
    }

    it("should count the rows where: isCompressed is true") {
      val count = eval("coll.countRows(_.isCompressed)", coll.device.countRows(_.isCompressed))
      assert(count == 0)
    }

    it("should test existence where: lastSale >= 100") {
      val lastSale_lt_500 = eval("coll.exists(_.lastSale >= 950)", coll.exists(_.lastSale >= 100))
      assert(lastSale_lt_500)
    }

    it("should filter for items where: lastSale < 100") {
      val items = eval("coll.filter(_.lastSale < 100)", coll.filter(_.lastSale < 100))
      assert(items.nonEmpty)
    }

    it("should filter for items where not: lastSale < 100") {
      val items = eval("coll.filterNot(_.lastSale < 100)", coll.filterNot(_.lastSale < 100))
      assert(items.nonEmpty)
    }

    it("should find one item where: lastSale > 500") {
      val items = eval("coll.find(_.lastSale > 500)", coll.find(_.lastSale > 500))
      assert(items.nonEmpty)
    }

    it("should indicate whether all items satisfy: lastSale < 1000") {
      val result = eval("coll.forall(_.lastSale < 1000))", coll.forall(_.lastSale < 1000))
      assert(result)
    }

    it("should retrieve one item by its offset (rowID)") {
      val rowID = randomURID(coll)
      val item_? = eval(f"coll.get($rowID)", coll.get(rowID))
      assert(item_?.nonEmpty)
    }

    it("should read an individual field value (via column index)") {
      val field = eval(f"coll.getField(rowID = 0, columnIndex = 0)", coll.device.getField(rowID = 0, columnIndex = 0))
      assert(field.metadata.`type` == StringType)
    }

    it("should read an individual field value (via column name)") {
      val field = eval(f"coll.getField(rowID = 0, column = 'lastSale)", coll.device.getField(rowID = 0, column = 'lastSale))
      assert(field.metadata.`type` == DoubleType)
    }

    it("should retrieve one row by its offset (rowID)") {
      val rowID = randomURID(coll)
      val row = eval(f"coll.getRow($rowID)", coll.device.getRow(rowID))

      logger.info(s"rowID: \t ${row.rowID}")
      logger.info(s"metadata: \t ${row.metadata}")
      row.fields.zipWithIndex foreach { case (field, index) =>
        logger.info(f"[$index%02d]: \t ${field.name} - ${field.metadata}")
      }
    }

    it("should retrieve one row metadata by its offset (rowID)") {
      val rowID = randomURID(coll)
      val rmd = eval(f"coll.getRowMetaData($rowID)", coll.device.readRowMetaData(rowID))
      logger.info(s"rmd => $rmd")
    }

    it("should retrieve the first item (sequentially) from the collection") {
      val coll = newCollection[StockQuote](partitionSize = 1)
      coll ++= stocks4
      val item_? = eval("coll.headOption", coll.headOption)
      assert(item_?.contains(stocks4.head))
    }

    it("""should find index where: symbol == "XXX"""") {
      coll.append(bonjourStock.copy(symbol = "XXX"))
      val index = eval("""coll.indexWhere(_.symbol == "XXX")""", coll.indexWhere(_.symbol == "XXX"))
      assert(index != -1)
    }

    it(s"""should find index of $bonjourStock""") {
      coll.append(bonjourStock)
      logger.info(s"coll.lastOption => ${coll.lastOption}")
      val index = eval(s"""coll.indexOf($bonjourStock)""", coll.indexOf(bonjourStock))
      assert(index != -1)
    }

    it("should produce an iterator") {
      val it = eval("coll.iterator", coll.iterator)
      assert(it.nonEmpty)
    }

    it("should retrieve the last item (sequentially) from the collection") {
      val coll = newCollection[StockQuote](partitionSize = 1)
      coll ++= stocks4
      val item_? = eval("coll.lastOption", coll.lastOption)
      assert(item_?.contains(stocks4.last))
    }

    it("should retrieve the file length") {
      val length = eval("coll.length", coll.length)
      assert(length >= coll.count())
    }

    it("loads items from text files") {
      def doIt(): PersistentSeq[StockQuoteWithID] = {
        newCollection[StockQuoteWithID](partitionSize = 10).loadTextFile(new File("./stocks.csv")) {
          _.split("[,]") match {
            case Array(symbol, exchange, price, date) =>
              Some(StockQuoteWithID(symbol, exchange, price.toDouble, date.toLong))
            case _ => None
          }
        }
      }

      val items = eval("""ps.loadTextFile(new File("./responses.csv")) { ... }""", doIt())
      items.filter(_.lastSale > 500).zipWithIndex.take(5).foreach { case (item, index) => logger.info(f"[${index + 1}%04d] $item") }
    }

    it("should compute max(lastSale)") {
      val coll = newCollection[StockQuote](partitionSize = 5)
      coll ++= stocks4
      val value = eval("coll.max(_.lastSale)", coll.max(_.lastSale))
      assert(value == 357.21)
    }

    it("should compute min(lastSale)") {
      val coll = newCollection[StockQuote](partitionSize = 5)
      coll ++= stocks4
      val value = eval("coll.min(_.lastSale)", coll.min(_.lastSale))
      assert(value == 22.92)
    }

    it("should compute the 95th percentile for lastSale") {
      val value = eval("coll.percentile(0.95)(_.lastSale)", coll.percentile(0.95)(_.lastSale))
      assert(value > 0)
    }

    it("should push items like a stack") {
      val stock = randomQuote
      val before = coll.count()
      eval(s"coll.push($stock)", coll.push(stock))
      val after = coll.count()
      assert(after - before == 1)
    }

    it("should pop items like a stack") {
      val before = coll.count()
      eval("coll.pop", coll.pop)
      val after = coll.count()
      assert(before - after == 1)
    }

    it("should retrieve the record size") {
      val recordSize = eval("coll.recordSize", coll.device.recordSize)
      assert(recordSize == 49)
    }

    it("should remove records by its offset (rowID)") {
      val rowID = randomURID(coll)
      val before = coll.count()
      eval(f"coll.remove($rowID)", coll.device.remove(rowID))
      val after = coll.count()
      assert(before - after == 1)
    }

    it("should remove records where: lastSale > 500") {
      val count = eval("coll.remove(_.lastSale > 500)", coll.remove(_.lastSale > 500))
      assert(count > 0)
    }

    it("should show that length and count() can be inconsistent") {
      info(s"coll.count() ~> ${coll.count()} but coll.length ~> ${coll.length}")
    }

    it("should reverse the collection") {
      val items = eval("coll.reverse", coll.reverse)
      assert(items.lastOption == coll.headOption)
      assert(items.headOption == coll.lastOption)
    }

    it("should produce a reverse iterator") {
      val it = eval("coll.reverseIterator", coll.reverseIterator)
      assert(it.hasNext)
      assert(it.toList.lastOption == coll.headOption)
    }

    it("should reverse the collection (in place)") {
      val coll = newCollection[StockQuote](partitionSize = 5)
      coll ++= stocks4
      eval("coll.reverseInPlace()", coll.device.reverseInPlace())
      assert(coll.toArray sameElements stocks4.reverse)
    }

    it("should extract a slice of items from the collection") {
      if (coll.nonEmpty) {
        val size = coll.length / 2
        val fromPos = new Random().nextInt(coll.length - size)
        val items = eval(f"coll.slice($fromPos, ${fromPos + size})", coll.slice(fromPos, fromPos + size))
        items.take(5).foreach(item => println(item.toString))
      }
    }

    it("should sort the collection") {
      val items = eval("coll.sortBy(_.symbol)", coll.sortBy(_.symbol))
      items.take(5).foreach(item => println(item.toString))
    }

    it("should sort the collection (in place)") {
      eval("coll.sortByInPlace(_.symbol)", coll.sortByInPlace(_.symbol))
      coll.take(5).foreach(item => println(item.toString))
    }

    it("should compute sum(lastSale)") {
      eval("coll.sum(_.lastSale)", coll.sum(_.lastSale))
    }

    it("should swap the position of two rows") {
      if (coll.nonEmpty) {
        val offset1: ROWID = randomURID(coll)
        val offset2: ROWID = randomURID(coll)
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"BEFORE: $item"))
        eval(s"coll.swap($offset1, $offset2)", coll.device.swap(offset1, offset2))
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"AFTER: $item"))
      }
    }

    it("should shrink the collection by 20%") {
      val newSize = (coll.count() * 0.80).toInt
      eval(f"coll.shrinkTo($newSize)", coll.device.shrinkTo(newSize))
      assert(coll.length <= newSize)
    }

    it("should tail a collection") {
      eval("coll.tail", coll.tail)
    }

    it("should trim dead entries from of the collection") {
      eval("(900 to 999).map(coll.remove)", (900 to 999).map(coll.device.remove))
      eval("coll.trim()", coll.device.trim())
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

}