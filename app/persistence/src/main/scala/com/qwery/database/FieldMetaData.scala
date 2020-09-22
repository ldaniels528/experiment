package com.qwery.database

import com.qwery.database.FieldMetadata._

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
case class FieldMetadata(isCompressed: Boolean,
                         isEncrypted: Boolean,
                         isNotNull: Boolean,
                         `type`: ColumnTypes.ColumnType) {

  /**
   * Encodes the [[FieldMetadata metadata]] into a bit sequence representing the metadata
   * @return a byte representing the metadata
   */
  def encode: Byte = {
    val c = if (isCompressed) COMPRESSED_BIT else 0
    val e = if (isEncrypted) ENCRYPTED_BIT else 0
    val n = if (isNotNull) NULLABLE_BIT else 0
    val t = `type`.id & TYPE_BITS
    (c | e | n | t).toByte
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
object FieldMetadata {
  // bit enumerations
  val COMPRESSED_BIT = 0x80
  val ENCRYPTED_BIT = 0x40
  val NULLABLE_BIT = 0x20
  val TYPE_BITS = 0x1f

  /**
   * Creates new field metadata based on existing column metadata
   * @param metadata the [[ColumnMetadata column metadata]]
   * @return new [[FieldMetadata field metadata]]
   */
  def apply(metadata: ColumnMetadata): FieldMetadata = FieldMetadata(
    isCompressed = metadata.isCompressed,
    isEncrypted = metadata.isEncrypted,
    isNotNull = true,
    `type` = metadata.`type`
  )

  /**
   * Decodes the 8-bit metadata code into a [[FieldMetadata metadata]]
   * @param metadataBits the metadata byte
   * @return a new [[FieldMetadata metadata]]
   */
  def decode(metadataBits: Byte): FieldMetadata = new FieldMetadata(
    isCompressed = (metadataBits & COMPRESSED_BIT) > 0,
    isEncrypted = (metadataBits & ENCRYPTED_BIT) > 0,
    isNotNull = (metadataBits & NULLABLE_BIT) > 0,
    `type` = ColumnTypes(metadataBits & TYPE_BITS)
  )

}