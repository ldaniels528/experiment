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
   * @return the maximum length of the column
   */
  def maxLength: Int

  /**
   * @return the [[ColumnMetadata column metadata]]
   */
  def metadata: ColumnMetadata

  /**
   * @return true if the column is a logic column
   */
  def isLogical: Boolean = metadata.isRowID

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |maxLength=$maxLength,
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
   * @param metadata the [[ColumnMetadata column metadata]]
   * @param maxSize  the optional maximum length of the column
   * @return a new [[Column]]
   */
  def apply(name: String,
            metadata: ColumnMetadata,
            maxSize: Option[Int]): Column = {
    val maxLength: Int = (metadata.`type`.getFixedLength ?? maxSize.map(_ + SHORT_BYTES)).map(_ + STATUS_BYTE)
      .getOrElse(throw new IllegalArgumentException(s"The maximum length of '$name' could not be determined for type ${metadata.`type`}"))
    DefaultColumn(name, metadata, maxLength)
  }

  /**
   * Unwraps the column
   * @param col the [[Column column]]
   * @return the option of the extracted values
   */
  def unapply(col: Column): Option[(String, ColumnMetadata, Int)] = Some((col.name, col.metadata, col.maxLength))

  /**
   * Represents a Column
   * @param name      the name of the column
   * @param metadata  the [[ColumnMetadata column metadata]]
   * @param maxLength the maximum length of the column
   */
  case class DefaultColumn(name: String,
                           metadata: ColumnMetadata,
                           maxLength: Int) extends Column

}