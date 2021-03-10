package com.qwery.database
package models

import com.qwery.database.models.ColumnTypes._
import com.qwery.models.Column
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Represents a table column
 * @param name         the name of the column
 * @param `type`       the [[ColumnType column type]]
 * @param sizeInBytes  the maximum size (in bytes) of the column
 * @param precision    the optional column precision
 * @param comment      the optional comment/remarks
 * @param defaultValue the optional default value
 * @param enumValues   the enumeration values (if any)
 * @param isCompressed indicates whether the data is to be compressed
 * @param isNullable   indicates whether the data is nullable
 * @param isRowID      indicates whether the column returns the row ID                
 */
case class TableColumn(name: String,
                       `type`: ColumnType,
                       sizeInBytes: Int,
                       precision: Option[Int] = None,
                       comment: Option[String] = None,
                       defaultValue: Option[String] = None,
                       enumValues: Seq[String] = Nil,
                       isCompressed: Boolean = false,
                       isNullable: Boolean = true,
                       isRowID: Boolean = false) {

  /**
   * @return true if the column is an enumeration type
   */
  def isEnum: Boolean = enumValues.nonEmpty

  /**
   * @return true if the column is an Array, BLOB, CLOB or Serializable
   */
  def isExternal: Boolean = `type` match {
    case ArrayType | BlobType | ClobType | SerializableType => true
    case _ => false
  }

  /**
   * @return true if the column is a non-persistent column
   */
  def isLogical: Boolean = isRowID

  /**
   * Returns the maximum physical size of the column
   */
  def maxPhysicalSize: Int = {
    val size = `type` match {
      case ArrayType => sizeInBytes + SHORT_BYTES
      case BlobType | ClobType | SerializableType => sizeInBytes + INT_BYTES
      case BigDecimalType => sizeInBytes + 2 * SHORT_BYTES
      case StringType if isEnum => SHORT_BYTES
      case StringType => sizeInBytes + SHORT_BYTES
      case _ => sizeInBytes
    }
    size + FieldMetadata.BYTES_LENGTH
  }

  override def toString: String = {
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |type=${`type`},
        |sizeInBytes=$sizeInBytes,
        |precision=${precision.getOrElse(0)},
        |comment=${comment.orNull},
        |defaultValue=${defaultValue.orNull},
        |enumValues=[${enumValues.map(s => s"'$s'").mkString(",")}],
        |isCompressed=$isCompressed,
        |isNullable=$isNullable,
        |isRowID=$isRowID
        |)""".stripMargin.split("\n").mkString
  }

}

/**
 * Table Column Companion
 */
object TableColumn {

  /**
   * Creates a new Column
   * @param name         the name of the column
   * @param `type`       the [[ColumnType column type]]
   * @param maxSize      the optional maximum length of the column
   * @param precision    the optional column precision
   * @param comment      the column's optional comment/documentation
   * @param defaultValue the optional default value
   * @param enumValues   the enumeration values (if any)
   * @param isCompressed indicates whether the data is compressed
   * @param isNullable   indicates whether the data is nullable
   * @param isRowID      indicates whether the column returns the row ID
   * @return a new [[TableColumn]]
   */
  def create(name: String,
             `type`: ColumnType,
             maxSize: Option[Int] = None,
             precision: Option[Int] = None,
             comment: Option[String] = None,
             defaultValue: Option[String] = None,
             enumValues: Seq[String] = Nil,
             isCompressed: Boolean = false,
             isNullable: Boolean = true,
             isRowID: Boolean = false): TableColumn = {
    val enumMaxLength = if (enumValues.nonEmpty) Some(enumValues.map(_.length).max) else None
    new TableColumn(
      name = name,
      `type` = `type`,
      sizeInBytes = enumMaxLength ?? `type`.getFixedLength ?? maxSize || INT_BYTES,
      precision = precision,
      comment = comment,
      defaultValue = defaultValue,
      enumValues = enumValues,
      isCompressed = isCompressed,
      isNullable = isNullable,
      isRowID = isRowID
    )
  }

  /**
    * Implicit classes and conversions
    */
  object implicits {

    /**
      * SQL Column-To-Column Conversion
      * @param column the [[Column SQL Column]]
      */
    final implicit class SQLToColumnConversion(val column: Column) extends AnyVal {

      @inline
      def toTableColumn: TableColumn = {
        TableColumn.create(
          name = column.name,
          `type` = lookupColumnType(column.spec.`type`),
          maxSize = column.spec.size,
          precision = column.spec.precision,
          comment = column.comment,
          defaultValue = column.defaultValue,
          enumValues = column.enumValues,
          isCompressed = column.isCompressed,
          isNullable = column.isNullable,
          isRowID = column.isRowID)
      }
    }

  }

}