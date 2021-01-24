package com.qwery.database
package collections

import com.qwery.database.device.BlockDevice
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.io.File
import scala.reflect.ClassTag

class ShrinkTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val expectedCount: Int = 10000

  describe(classOf[PersistentSeq[_]].getSimpleName) {

    // build the collection
    val coll = newCollection[StockQuoteWithID](partitionSize = expectedCount / 11)
      .loadTextFile(new File("./stocks.csv")) {
        _.split("[,]") match {
          case Array(symbol, exchange, price, date) =>
            Some(StockQuoteWithID(symbol, exchange, price.toDouble, date.toLong))
          case _ => None
        }
      }

    logger.info(s"coll is a ${coll.getClass.getSimpleName}; sample size is $expectedCount")

    it("should shrink the collection by 20%") {
      val newSize = (coll.count() * 0.80).toInt
      eval(f"coll.shrinkTo($newSize)", coll.device.shrinkTo(newSize))
      assert(coll.length <= newSize)
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

  def newCollection[A <: Product : ClassTag](partitionSize: Int): PersistentSeq[A] = {
    PersistentSeq[A](
      BlockDevice.builder
        .withRowModel[A]
        //.withMemoryCapacity(capacity = (expectedCount * 1.2).toInt)
        //.withParallelism(ExecutionContext.global)
        .withPartitions(partitionSize))

  }

}
