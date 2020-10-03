package com.qwery

import java.nio.ByteBuffer

import scala.reflect.ClassTag

/**
 * Qwery database package object
 */
package object database {
  type Block = (ROWID, ByteBuffer)
  type KeyValue = (String, Option[Any])
  type RECORD_ID = Int
  type ROWID = Int

  // byte quantities
  val ONE_BYTE = 1
  val INT_BYTES = 4
  val LONG_BYTES = 8
  val ROW_ID_BYTES = 4
  val SHORT_BYTES = 2

  // status bits
  val STATUS_BYTE = 1

  def safeCast[T: ClassTag](x: Any): Option[T] = x match {
    case v: T => Some(v)
    case _ =>  Option.empty[T]
  }

  /**
   * Math Utilities for Long integers
   * @param number the long integer
   */
  final implicit class MathUtilsLong(val number: Long) extends AnyVal {

    def toRowID: ROWID = number.toInt

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
