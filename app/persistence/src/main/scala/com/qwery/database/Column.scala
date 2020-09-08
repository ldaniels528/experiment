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
   * @return indicates whether this column is to be automatically compressed
   */
  def isCompressed: Boolean

  /**
   * @return indicates whether column is to be automatically encrypted
   */
  def isEncrypted: Boolean

  /**
   * @return indicates whether column may contain nulls
   */
  def isNullable: Boolean

  /**
   * @return indicates whether this column is a primary key
   */
  def isPrimary: Boolean

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |isCompressed=$isCompressed,
        |isEncrypted=$isEncrypted,
        |isNullable=$isNullable,
        |isPrimary=$isPrimary,
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
   * @param isCompressed indicates whether this column is to be automatically compressed
   * @param isEncrypted  indicates whether this column is to be automatically encrypted
   * @param isNullable   indicates whether column may contain nulls
   * @param isPrimary    indicates whether this column is a primary key
   * @return a new [[Column]]
   */
  def apply(name: String,
            `type`: ColumnTypes.ColumnType,
            maxSize: Option[Int],
            isCompressed: Boolean,
            isEncrypted: Boolean,
            isNullable: Boolean,
            isPrimary: Boolean): Column = {
    val maxLength: Int = (`type`.getFixedLength ?? maxSize.map(_ + SHORT_BYTES)).map(_ + STATUS_BYTE)
      .getOrElse(throw new IllegalArgumentException(s"The maximum length of '$name' could not be determined for type ${`type`}"))
    DefaultColumn(name, `type`, maxLength, isCompressed, isEncrypted, isNullable, isPrimary)
  }

  /**
   * Unwraps the column
   * @param column the [[Column]]
   * @return the option of the extracted values
   */
  def unapply(column: Column): Option[(String, ColumnTypes.ColumnType, Int, Boolean, Boolean, Boolean)] = {
    Some((column.name, column.`type`, column.maxLength, column.isCompressed, column.isEncrypted, column.isPrimary))
  }

  /**
   * Represents a Column
   * @param name         the name of the column
   * @param `type`       the [[ColumnTypes.ColumnType type]] of the column
   * @param maxLength    the maximum length of the column
   * @param isCompressed indicates whether this column is to be automatically compressed
   * @param isEncrypted  indicates whether this column is to be automatically encrypted
   * @param isNullable   indicates whether column may contain nulls
   * @param isPrimary    indicates whether this column is a primary key
   */
  case class DefaultColumn(name: String,
                           `type`: ColumnTypes.ColumnType,
                           maxLength: Int,
                           isCompressed: Boolean,
                           isEncrypted: Boolean,
                           isNullable: Boolean,
                           isPrimary: Boolean) extends Column

}