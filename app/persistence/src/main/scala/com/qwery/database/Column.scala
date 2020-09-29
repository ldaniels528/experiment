package com.qwery.database

import com.qwery.database.ColumnTypes.{ArrayType, BigDecimalType, BlobType, StringType}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Represents a Column
 * @param name        the name of the column
 * @param comment     the column's optional comment/documentation
 * @param metadata    the [[ColumnMetadata column metadata]]
 * @param sizeInBytes the maximum size (in bytes) of the column
 */
case class Column(name: String, comment: String, metadata: ColumnMetadata, sizeInBytes: Int) {

  /**
   * @return true if the column is a non-persistent column
   */
  def isLogical: Boolean = metadata.isRowID

  /**
   * Returns the maximum physical size of the column
   */
  val maxPhysicalSize: Int = {
    val size = metadata.`type` match {
      case ArrayType => sizeInBytes + SHORT_BYTES
      case BlobType => sizeInBytes + INT_BYTES
      case BigDecimalType => sizeInBytes + 2 * SHORT_BYTES
      case StringType => sizeInBytes + SHORT_BYTES
      case _ => sizeInBytes
    }
    size + STATUS_BYTE
  }

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |sizeInBytes=$sizeInBytes,
        |metadata=$metadata
        |)""".stripMargin.split("\n").mkString

}

/**
 * Column Companion
 */
object Column {

  /**
   * Creates a new Column
   * @param name     the name of the column
   * @param comment  the column's optional comment/documentation
   * @param metadata the [[ColumnMetadata column metadata]]
   * @param maxSize  the optional maximum length of the column
   * @return a new [[Column]]
   */
  def apply(name: String, comment: String, metadata: ColumnMetadata, maxSize: Option[Int] = None): Column = {
    new Column(name, comment, metadata, sizeInBytes = (metadata.`type`.getFixedLength ?? maxSize)
      .getOrElse(throw new IllegalArgumentException(s"The maximum length of '$name' could not be determined for type ${metadata.`type`}")))
  }

}