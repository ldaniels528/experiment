package com.qwery.database

import com.qwery.util.OptionHelper._

/**
 * Represents a Column
 */
trait Column {

  /**
   * @return the name of the column
   */
  def name: String

  /**
   * @return the type of the column
   */
  def `type`: ColumnTypes.ColumnType

  /**
   * @return the maximum length of the column
   */
  def maxLength: Int

  /**
   * @return true if the column is to be automatically compressed
   */
  def isCompressed: Boolean

  /**
   * @return true if the column is to be automatically encrypted
   */
  def isEncrypted: Boolean

  /**
   * @return true if the column may contain nulls
   */
  def isNullable: Boolean

  /**
   * @return true if the column is a primary key
   */
  def isPrimary: Boolean

  /**
   * @return true if the column is an offset/row identifier
   */
  def isRowID: Boolean

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |isCompressed=$isCompressed,
        |isEncrypted=$isEncrypted,
        |isNullable=$isNullable,
        |isPrimary=$isPrimary,
        |isRowID=$isRowID,
        |maxLength=$maxLength,
        |type=${`type`}
        |)""".stripMargin.split("\n").mkString

}

/**
 * Column Companion
 */
object Column {

  /**
   * Creates a new Column
   * @param name         the name of the column
   * @param `type`       the [[ColumnTypes.ColumnType type]] of the column
   * @param maxSize      the optional maximum length of the column
   * @param isCompressed true if the column is to be automatically compressed
   * @param isEncrypted  true if the column is to be automatically encrypted
   * @param isNullable   true if the column may contain nulls
   * @param isPrimary    true if the column is a primary key
   * @param isRowID      true if the column is an offset/row identifier
   * @return a new [[Column]]
   */
  def apply(name: String,
            `type`: ColumnTypes.ColumnType,
            maxSize: Option[Int],
            isCompressed: Boolean,
            isEncrypted: Boolean,
            isNullable: Boolean,
            isPrimary: Boolean,
            isRowID: Boolean): Column = {
    val maxLength: Int = (`type`.getFixedLength ?? maxSize.map(_ + SHORT_BYTES)).map(_ + STATUS_BYTE)
      .getOrElse(throw new IllegalArgumentException(s"The maximum length of '$name' could not be determined for type ${`type`}"))
    DefaultColumn(name, `type`, maxLength, isCompressed, isEncrypted, isNullable, isPrimary, isRowID)
  }

  /**
   * Unwraps the column
   * @param col the [[Column column]]
   * @return the option of the extracted values
   */
  def unapply(col: Column): Option[(String, ColumnTypes.ColumnType, Int, Boolean, Boolean, Boolean, Boolean)] = {
    Some((col.name, col.`type`, col.maxLength, col.isCompressed, col.isEncrypted, col.isPrimary, col.isRowID))
  }

  /**
   * Represents a Column
   * @param name         the name of the column
   * @param `type`       the [[ColumnTypes.ColumnType type]] of the column
   * @param maxLength    the maximum length of the column
   * @param isCompressed true if the column is to be automatically compressed
   * @param isEncrypted  true if the column is to be automatically encrypted
   * @param isNullable   true if the column may contain nulls
   * @param isPrimary    true if the column is a primary key
   * @param isRowID      true if the column is an offset/row identifier
   */
  case class DefaultColumn(name: String,
                           `type`: ColumnTypes.ColumnType,
                           maxLength: Int,
                           isCompressed: Boolean,
                           isEncrypted: Boolean,
                           isNullable: Boolean,
                           isPrimary: Boolean,
                           isRowID: Boolean) extends Column

}