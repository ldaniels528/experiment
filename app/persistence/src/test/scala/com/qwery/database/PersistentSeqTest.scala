package com.qwery.database

import java.io.File

import com.qwery.database.PersistentSeqTest.StockQuoteWithID
import com.qwery.database.StockQuote._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.annotation.meta.field
import scala.util.Random

/**
 * Persistent Sequence Test Suite
 */
class PersistentSeqTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val expectedCount: Int = 50000
  //private val coll = PersistentSeq[StockQuote]()
  //private val coll = PersistentSeq[StockQuote](expectedCount / 2)
  private val coll = new PartitionedPersistentSeq[StockQuote](partitionSize = 5000)

  private val stocks: Seq[StockQuote] = (1 to expectedCount) map { _ => randomQuote }
  private val bonjourStock = randomQuote.copy(symbol = "BONJOUR")

  describe(classOf[PersistentSeq[_]].getSimpleName) {

    it("should start with an empty collection") {
      val items = eval("coll.clear()", coll.clear())
      assert(items.isEmpty)
    }

    it("should append a collection of items to the collection") {
     val items = eval("coll.append(Seq(..))", coll.append(stocks))
      assert(items.count() == expectedCount)
    }

    it("should append an item to the collection") {
      val items = eval(s"coll.append($bonjourStock)", coll.append(bonjourStock))
      assert(items.count() == 1 + expectedCount)
    }

    it("should retrieve one record with row metadata by its offset (_id)") {
      val _id = randomURID(coll)
      eval(f"coll.apply(${_id})", coll(_id))
    }

    it("should compute avg(lastSale)") {
      eval("coll.avg(_.lastSale)", coll.avg(_.lastSale))
    }

    it("should count all items") {
      eval("coll.count()", coll.count())
    }

    it("should count the items where: lastSale <= 500") {
      eval("coll.count(_.lastSale <= 500))", coll.count(_.lastSale <= 500))
    }

    it("should count the rows where: isEncrypted is true") {
      eval("coll.countRows(_.isEncrypted)", coll.countRows(_.isEncrypted))
    }

    it("should test existence where: lastSale >= 500") {
      eval("coll.exists(_.lastSale >= 500)", coll.exists(_.lastSale >= 500))
    }

    it("should filter for items where: lastSale <= 5") {
      eval("coll.filter(_.lastSale <= 5)", coll.filter(_.lastSale <= 5))
    }

    it("should filter for items where not: lastSale <= 5") {
      eval("coll.filterNot(_.lastSale <= 5)", coll.filterNot(_.lastSale <= 5))
    }

    it("should find one item where: lastSale > 500") {
      eval("coll.find(_.lastSale > 500)", coll.find(_.lastSale > 500))
    }

    it("should indicate whether all items satisfy: lastSale < 1000") {
      eval("coll.forall(_.lastSale < 1000))", coll.forall(_.lastSale < 1000))
    }

    it("should retrieve one item by its offset (_id)") {
      val _id = randomURID(coll)
      eval(f"coll.get(${_id})", coll.get(_id))
    }

    it("should retrieve one row by its offset (_id)") {
      val _id = randomURID(coll)
      val row = eval(f"coll.getRow(${_id})", coll.getRow(_id))

      logger.info(s"_id: \t ${row._id}")
      logger.info(s"metadata: \t ${row.metadata}")
      row.fields.zipWithIndex foreach { case (field, index) =>
        logger.info(f"[$index%02d]: \t ${field.name} - ${field.metadata}")
      }
    }

    it("should retrieve one row metadata by its offset (_id)") {
      val _id = randomURID(coll)
      eval(f"coll.getRowMetaData(${_id})", coll.getRowMetaData(_id))
    }

    it("should retrieve the first item (sequentially) from the collection") {
      eval("coll.headOption", coll.headOption)
    }

    it("""should find index where: symbol == "BONJOUR"""") {
      eval("""coll.indexWhere(_.symbol == "BONJOUR")""", coll.indexWhere(_.symbol == "BONJOUR"))
    }

    it(s"""should find index of $bonjourStock""") {
      eval(s"""coll.indexOf($bonjourStock)""", coll.indexOf(bonjourStock))
    }

    it("should retrieve the last item (sequentially) from the collection") {
      eval("coll.lastOption", coll.lastOption)
    }

    it("should retrieve the file length") {
      eval("coll.length", coll.length)
    }

    it("loads items from text files") {
      def doIt(): PersistentSeq[StockQuoteWithID] = {
        PersistentSeq[StockQuoteWithID]().loadTextFile(new File("./stocks.csv")) {
          _.split("[,]") match {
            case Array(symbol, exchange, price, date) =>
              Some(StockQuoteWithID(symbol, exchange, price.toDouble, date.toLong))
            case _ => None
          }
        }
      }

      val items = eval("""ps.loadTextFile(new File("./responses.csv")) { ... }""", doIt())
      items.zipWithIndex({ (item, _) => item }).take(5).foreach(item => logger.info(f"[${item._id}%04d] $item"))
    }

    it("should compute max(lastSale)") {
      eval("coll.max(_.lastSale)", coll.max(_.lastSale))
    }

    it("should compute min(lastSale)") {
      eval("coll.min(_.lastSale)", coll.min(_.lastSale))
    }

    it("should compute the 95th percentile for lastSale") {
      eval("coll.percentile(0.95)(_.lastSale)", coll.percentile(0.95)(_.lastSale))
    }

    it("should push items like a stack") {
      val stock = randomQuote
      eval(s"coll.push($stock)", coll.push(stock))
    }

    it("should pop items like a stack") {
      eval("coll.pop", coll.pop)
    }

    it("should retrieve the record size") {
      eval("coll.recordSize", coll.recordSize)
    }

    it("should remove records by its offset (_id)") {
      val _id = randomURID(coll)
      eval(f"coll.remove(${_id})", coll.remove(_id))
    }

    it("should remove records where: lastSale > 500") {
      eval("coll.remove(_.lastSale > 500)", coll.remove(_.lastSale > 500))
    }

    it("should show that length and count() can be inconsistent") {
      info(s"coll.count() ~> ${coll.count()} but coll.length ~> ${coll.length}")
    }

    it("should compact the database to eliminate deleted rows") {
      eval("coll.compact()", coll.compact())
    }

    it("should reverse the collection") {
      eval("coll.reverse", coll.reverse)
    }

    it("should reverse the collection (in place)") {
      eval("coll.reverseInPlace()", coll.reverseInPlace())
    }

    it("should extract a slice of items from the collection") {
      if (coll.nonEmpty) {
        val size = coll.length / 2
        val fromPos = new Random().nextInt(coll.length - size)
        eval(f"coll.slice($fromPos, ${fromPos + size})", coll.slice(fromPos, fromPos + size))
      }
    }

    it("should sort the collection") {
      val items = eval("coll.sortBy(_.symbol)", coll.sortBy(_.symbol))
      for (item <- items.take(0, 5)) info(item.toString)
    }

    it("should compute sum(lastSale)") {
      eval("coll.sum(_.lastSale)", coll.sum(_.lastSale))
    }

    it("should swap the position of two rows") {
      if (coll.nonEmpty) {
        val offset1: URID = randomURID(coll)
        val offset2: URID = randomURID(coll)
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"BEFORE: $item"))
        eval(s"coll.swap($offset1, $offset2)", coll.swap(offset1, offset2))
        Seq(offset1, offset2).flatMap(coll.get).foreach(item => logger.info(s"AFTER: $item"))
      }
    }

    it("should tail a collection") {
      eval("coll.tail", coll.tail)
    }

    it("should shrink the collection by 20%") {
      val newSize = (coll.length * 0.80).toInt
      eval(f"coll.shrinkTo($newSize)", coll.shrinkTo(newSize))
    }

    it("should produce an iterator") {
      eval("coll.toIterator", coll.toIterator)
    }

    it("should trim dead entries from of the collection") {
      eval("(900 to 999).map(coll.remove)", (900 to 999).map(coll.remove))
      eval("coll.trim()", coll.trim())
    }

  }

  def eval[A](label: String, f: => A): A = {
    val (results, runTime) = time(f)
    val output = results match {
      case items: PersistentSeq[_] => f"(${items.length} items)"
      case value: Double => f"$value%.2f"
      case items: Seq[_] => f"(${items.length} items)"
      case it: Iterator[_] => if (it.hasNext) s"<${it.next()}, ...>" else "<empty>"
      case x => x.toString
    }
    logger.info(f"$label ~> $output [$runTime%.1f msec]")
    results
  }

  /**
   * Executes the block capturing the execution time
   * @param block the block to execute
   * @tparam T the result type
   * @return a tuple containing the result and execution time in milliseconds
   */
  def time[T](block: => T): (T, Double) = {
    val startTime = System.nanoTime()
    val result = block
    val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
    (result, elapsedTime)
  }

}

/**
 * Persistent Sequence Test Companion
 */
object PersistentSeqTest {

  case class StockQuoteWithID(@(ColumnInfo@field)(maxSize = 12) symbol: String,
                              @(ColumnInfo@field)(maxSize = 12) exchange: String,
                              lastSale: Double,
                              lastSaleTime: Long,
                              _id: URID = 0)

}