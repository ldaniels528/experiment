package com.qwery.database

import com.qwery.database.FieldMetaData._
import com.qwery.database.PersistentSeq.Field

/**
 * Represents the metadata of a field stored in the database.
 * <pre>
 * ----------------------------------------
 * c - compressed bit .. [1000.0000 ~ 0x80]
 * e - encrypted bit ... [0100.0000 ~ 0x40]
 * n - nullable bit .... [0010.0000 ~ 0x20]
 * t - type bits (x5) .. [0001.1111 ~ 0x1f]
 * ----------------------------------------
 * </pre>
 * @param isCompressed indicates whether the data is compressed
 * @param isEncrypted  indicates whether the data is encrypted
 * @param isNotNull    indicates whether the data is available; meaning not null.
 * @param `type`       the [[ColumnTypes.ColumnType column type]]
 */
case class FieldMetaData(isCompressed: Boolean,
                         isEncrypted: Boolean,
                         isNotNull: Boolean,
                         `type`: ColumnTypes.ColumnType) {

  /**
   * Encodes the [[FieldMetaData metadata]] into a byte representing the metadata
   * @return a byte representing the metadata
   */
  def encode: Int = {
    val c = if (isCompressed) COMPRESSED_BIT else 0
    val e = if (isEncrypted) ENCRYPTED_BIT else 0
    val n = if (isNotNull) NOT_NULL_BIT else 0
    val t = `type`.id & TYPE_BITS
    c | e | n | t
  }

  def isNull: Boolean = !isNotNull

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |isCompressed=$isCompressed,
        |isEncrypted=$isEncrypted,
        |isNotNull=$isNotNull,
        |type=${`type`}
        |)""".stripMargin.split("\n").mkString

}

/**
 * Field MetaData Companion
 */
object FieldMetaData {
  // bit enumerations
  val COMPRESSED_BIT = 0x80
  val ENCRYPTED_BIT = 0x40
  val NOT_NULL_BIT = 0x20
  val TYPE_BITS = 0x1f

  /**
   * Creates new field meta data based on the column
   * @param column the [[Column column]]
   * @return new [[FieldMetaData field meta data]]
   */
  def apply(column: Column): FieldMetaData = FieldMetaData(
    isCompressed = column.isCompressed,
    isEncrypted = column.isEncrypted,
    isNotNull = true,
    `type` = column.`type`
  )

  /**
   * A template for generating read-only row "_id" fields
   */
  def _idField(offset: URID): Field = Field(name = "_id", value = Some(offset), metadata = FieldMetaData(
    isCompressed = false,
    isEncrypted = false,
    isNotNull = true,
    `type` = ColumnTypes.LongType
  ))

  /**
   * Decodes the metadata byte into a [[FieldMetaData metadata]]
   * @param metadataBits the metadata byte
   * @return a new [[FieldMetaData metadata]]
   */
  def decode(metadataBits: Byte): FieldMetaData = new FieldMetaData(
    isCompressed = (metadataBits & COMPRESSED_BIT) > 0,
    isEncrypted = (metadataBits & ENCRYPTED_BIT) > 0,
    isNotNull = (metadataBits & NOT_NULL_BIT) > 0,
    `type` = ColumnTypes(metadataBits & TYPE_BITS)
  )

}