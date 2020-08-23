package com.qwery

import java.nio.ByteBuffer

package object database {
  type Block = (URID, ByteBuffer)
  type KeyValue = (String, Option[Any])
  type URID = Int

  // byte quantities
  val ONE_BYTE = 1
  val INT_BYTES = 4
  val LONG_BYTES = 8
  val SHORT_BYTES = 2

  // status bits
  val STATUS_BYTE = 1

  final implicit class PersistentSeqArrayExtensions[T](val array: Array[T]) extends AnyVal {

    def quickSort[B <: Comparable[B]](predicate: T => B): array.type = {

      def partition(low: Int, high: Int)(f: T => B): Int = {
        val pivot = array(high)
        var i = low - 1 // index of lesser item
        for (j <- low until high) {
          val item = array(j)
          if (f(item).compareTo(f(pivot)) < 0) {
            i += 1 // increment the index of lesser item
            swap(i, j)
          }
        }
        swap(i + 1, high)
        i + 1
      }

      def doSorting(low: Int, high: Int)(f: T => B): Unit = {
        if (low < high) {
          val pi = partition(low, high)(f)
          doSorting(low, pi - 1)(f)
          doSorting(pi + 1, high)(f)
        }
      }

      def swap(i: Int, j: Int): Unit = {
        val temp = array(i)
        array(i) = array(j)
        array(j) = temp
      }

      doSorting(low = 0, high = array.length - 1)(predicate)
      array
    }

  }

  final implicit class MathUtilsLong(val number: Long) extends AnyVal {

    def toURID: URID = number.toInt

    def isPrime: Boolean = number match {
      case n if n < 2 => false
      case 2 => true
      case n =>
        var m: Long = 1L
        while (m < n / m) {
          m += 1
          if (n % m == 0) return false
        }
        true
    }

  }

}
