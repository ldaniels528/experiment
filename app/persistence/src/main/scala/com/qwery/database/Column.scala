package com.qwery.database

import com.qwery.database.ColumnTypes.{ArrayType, BigDecimalType, BlobType, ClobType, SerializableType, StringType}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Represents a Column
 * @param name        the name of the column
 * @param comment     the column's optional comment/documentation
 * @param metadata    the [[ColumnMetadata column metadata]]
 * @param sizeInBytes the maximum size (in bytes) of the column
 * @param enumValues  the enumeration values (if any)
 */
case class Column(name: String,
                  comment: String,
                  enumValues: Seq[String],
                  metadata: ColumnMetadata,
                  sizeInBytes: Int) {

  /**
   * @return true if the column is an enumeration type
   */
  def isEnum: Boolean = enumValues.nonEmpty

  /**
   * @return true if the column is an Array, BLOB, CLOB or Serializable
   */
  def isExternal: Boolean = metadata.`type` match {
    case ArrayType | BlobType | ClobType | SerializableType => true
    case _ => false
  }

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
      case BlobType | ClobType | SerializableType => sizeInBytes + INT_BYTES
      case BigDecimalType => sizeInBytes + 2 * SHORT_BYTES
      case StringType if isEnum => SHORT_BYTES
      case StringType => sizeInBytes + SHORT_BYTES
      case _ => sizeInBytes
    }
    size + FieldMetadata.BYTES_LENGTH
  }

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |name=$name,
        |comment=$comment,
        |enumValues=${enumValues.map(s => s"'$s'").mkString(",")},
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
  def apply(name: String,
            comment: String = "",
            enumValues: Seq[String] = Nil,
            metadata: ColumnMetadata,
            maxSize: Option[Int] = None): Column = {
    val enumMaxLength = if (enumValues.nonEmpty) Some(enumValues.map(_.length).max) else None
    new Column(name, comment, enumValues, metadata, sizeInBytes = (metadata.`type`.getFixedLength ?? enumMaxLength ?? maxSize)
      .getOrElse(die(s"The maximum length of '$name' could not be determined for type ${metadata.`type`}")))
  }

}