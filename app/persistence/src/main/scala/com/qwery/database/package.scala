package com.qwery

import java.io.File
import java.nio.ByteBuffer

import com.qwery.database.ColumnTypes.ColumnType
import com.qwery.database.device.{BlockDevice, RowOrientedFileBlockDevice}

/**
 * Qwery database package object
 */
package object database {
  type Block = (ROWID, ByteBuffer)
  type KeyValue = (String, Option[Any])
  type RECORD_ID = Int

  // byte quantities
  val ONE_BYTE = 1
  val INT_BYTES = 4
  val LONG_BYTES = 8
  val ROW_ID_BYTES = 4
  val SHORT_BYTES = 2

  // row ID-related
  type ROWID = Int
  val ROWID_NAME = "__id"

  //////////////////////////////////////////////////////////////////////////////////////
  //  SERVER CONFIG
  //////////////////////////////////////////////////////////////////////////////////////

  def getServerRootDirectory: File = {
    val directory = new File(sys.env.getOrElse("QWERY_DB", "qwery_db"))
    assert(directory.mkdirs() || directory.exists(), throw DataDirectoryNotFoundException(directory))
    directory
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  TEMPORARY FILES AND TABLES
  //////////////////////////////////////////////////////////////////////////////////////

  def createTempFile(): File = {
    val file = File.createTempFile("qwery", ".lldb")
    file.deleteOnExit()
    file
  }

  def createTempTable(columns: Seq[Column]): BlockDevice = {
    new RowOrientedFileBlockDevice(columns, createTempFile())
  }

  def createTempTable(device: BlockDevice): BlockDevice = {
    new RowOrientedFileBlockDevice(device.columns, createTempFile())
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      UTILITIES
  /////////////////////////////////////////////////////////////////////////////////////////////////

  def die[A](message: => String): A = throw new RuntimeException(message)

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

  type StopWatch = () => Double

  def stopWatch: StopWatch = {
    val startTime = System.nanoTime()
    () => (System.nanoTime() - startTime) / 1e+6
  }

  /**
   * Executes the block capturing its execution the time in milliseconds
   * @param block the block to execute
   * @return a tuple containing the result of the block and its execution the time in milliseconds
   */
  def time[A](block: => A): (A, Double) = {
    val clock = stopWatch
    (block, clock())
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      EXCEPTIONS
  /////////////////////////////////////////////////////////////////////////////////////////////////

  case class ColumnCapacityExceededException(column: Column, fieldLength: Int)
    extends RuntimeException(s"Column '${column.name}' is too long: $fieldLength > ${column.maxPhysicalSize}")

  case class ColumnNotFoundException(tableName: String, columnName: String)
    extends RuntimeException(s"Column '$columnName' does not exist in $tableName")

  case class ColumnOutOfRangeException(columnIndex: Int)
    extends RuntimeException(s"Column index is out of range: $columnIndex")

  case class DataDirectoryNotFoundException(directory: File)
    extends RuntimeException(s"Could not create or find the data directory: ${directory.getAbsolutePath}")

  case class OffsetOutOfRangeException(offset: RECORD_ID, limit: RECORD_ID)
    extends RuntimeException(s"Maximum capacity exceeded: $offset > $limit")

  case class PartitionSizeException(partitionSize: Int)
    extends RuntimeException(s"Partition size must be greater than zero: $partitionSize")

  case class RowIsLockedException(rowIndex: ROWID)
    extends RuntimeException(s"Row ID is locked for write: $rowIndex")

  case class RowOutOfRangeException(rowIndex: ROWID)
    extends RuntimeException(s"Row ID is out of range: $rowIndex")

  case class TypeConversionException(value: Any, toType: ColumnType)
    extends RuntimeException(s"Failed to convert '$value' to $toType")

  case class UnsupportedFeature(featureName: String)
    extends RuntimeException(s"Feature '$featureName' is not supported")

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //      IMPLICIT CLASSES
  /////////////////////////////////////////////////////////////////////////////////////////////////

  final implicit class RicherBoolean(val boolean: Boolean) extends AnyVal {
    @inline def toInt: Int = if (boolean) 1 else 0
  }

  /**
   * Math Utilities for Long integers
   * @param number the long integer
   */
  final implicit class MathUtilsLong(val number: Long) extends AnyVal {

    def toBoolean: Boolean = number != 0

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

  /**
   * Recursive File List Enrichment
   * @param theFile the [[File file]]
   */
  final implicit class RecursiveFileList(val theFile: File) extends AnyVal {

    /**
     * Recursively retrieves all files
     * @return the list of [[File files]]
     */
    def listFilesRecursively: List[File] = theFile match {
      case directory if directory.isDirectory => directory.listFiles().toList.flatMap(_.listFilesRecursively)
      case file => file :: Nil
    }

  }

}
