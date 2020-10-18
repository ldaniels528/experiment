package com.qwery

import java.io.File
import java.nio.ByteBuffer

import com.qwery.database.ColumnTypes.ColumnType

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

  /**
   * Returns the option of the given value as the desired type
   * @param value the given value
   * @tparam T the desired type
   * @return the option of the given value casted as the desired type
   */
  def safeCast[T](value: Any): Option[T] = value match {
    case v: T => Some(v)
    case _ => Option.empty[T]
  }

  case class ColumnCapacityExceededException(column: Column, fieldLength: Int)
    extends RuntimeException(s"Column '${column.name}' is too long: $fieldLength > ${column.maxPhysicalSize}")

  case class ColumnOutOfRangeException(columnIndex: Int)
    extends RuntimeException(s"Column index is out of range: $columnIndex")

  case class DataDirectoryNotFoundException(directory: File)
    extends RuntimeException(s"Could not create or find the data directory: ${directory.getAbsolutePath}")

  case class OffsetOutOfRangeException(offset: RECORD_ID, limit: RECORD_ID)
    extends RuntimeException(s"Maximum capacity exceeded: $offset > $limit")

  case class PartitionSizeException(partitionSize: Int)
    extends RuntimeException(s"Partition size must be greater than zero: $partitionSize")

  case class RowOutOfRangeException(rowIndex: ROWID)
    extends RuntimeException(s"Row ID is out of range: $rowIndex")

  case class TypeConversionException(value: Any, toType: ColumnType)
    extends RuntimeException(s"Failed to convert '$value' to $toType")

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
