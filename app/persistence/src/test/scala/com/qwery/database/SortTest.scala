package com.qwery.database

import com.qwery.database.SortTest.QuickSortedArrays
import org.scalatest.funspec.AnyFunSpec

import scala.util.Random

class SortTest extends AnyFunSpec {

  describe(classOf[SortTest].getSimpleName) {

    it("should do something") {
      val random = new Random()
      val numbers = (0 to 20).map(_ => random.nextInt(10000) + 1).toArray
      println(s"numbers: ${numbers.toList}")
      val (_, sortTime) = time(numbers.quickSort())
      println(f"numbers: ${numbers.toList} in $sortTime%.1f msec")
    }
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

  /*
        import com.qwery.util.ResourceHelper._
      new PrintWriter(new FileWriter("./stocks.csv")) use { out =>
        items.foreach { item =>
          out.println(f"${item.symbol},${item.exchange},${item.lastSale}%.2f,${item.lastSaleTime}")
        }
        out.flush()
      }
   */

}

object SortTest {

 final implicit class QuickSortedArrays(val array: Array[Int]) extends AnyVal {

    def quickSort(low: Int = 0, high: Int = array.length - 1): Unit = {
      if (low < high) {
        val pi = array.partition(low, high)
        quickSort(low, pi - 1)
        quickSort(pi + 1, high)
      }
    }

    def partition(low: Int, high: Int): Int = {
      // pivot (Element to be placed at right position)
      val pivot = array(high)
      var i = low - 1 // Index of smaller element
      for (j <- low until high) {
        // If current element is smaller than the pivot
        if (array(j) < pivot) {
          i += 1 // increment index of smaller element
          array.swap(i, j)
        }
      }
      array.swap(i + 1, high)
      i + 1
    }

    def swap(i: Int, j: Int): Unit = {
      val tmp = array(i)
      array(i) = array(j)
      array(j) = tmp
    }

  }

}