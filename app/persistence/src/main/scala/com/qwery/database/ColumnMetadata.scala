package com.qwery.database

import com.qwery.database.ColumnMetadata._

/**
 * Represents the metadata of a database column.
 * <pre>
 * ----------------------------------------------------
 * x - external bit .... [0000.0100.0000.0000 ~ 0x0400]
 * c - compressed bit .. [0000.0010.0000.0000 ~ 0x0200]
 * e - encrypted bit ... [0000.0001.0000.0000 ~ 0x0100]
 * n - nullable bit .... [0000.0000.1000.0000 ~ 0x0080]
 * p - primary bit ..... [0000.0000.0100.0000 ~ 0x0040]
 * r - row ID bit ...... [0000.0000.0010.0000 ~ 0x0020]
 * t - type bits (x4) .. [0000.0000.0000.1111 ~ 0x000f]
 * ----------------------------------------------------
 * </pre>
 * @param `type`       the [[ColumnTypes.ColumnType column type]]
 * @param isCompressed indicates whether the data is compressed
 * @param isEncrypted  indicates whether the data is encrypted
 * @param isExternal   indicates whether the data is external (e.g. BLOB)
 * @param isNullable   indicates whether the data is nullable
 * @param isPrimary    indicates whether the column is part of the primary key
 * @param isRowID      indicates whether the column returns the row ID
 */
case class ColumnMetadata(`type`: ColumnTypes.ColumnType,
                          isCompressed: Boolean = false,
                          isEncrypted: Boolean = false,
                          isExternal: Boolean = false,
                          isNullable: Boolean = true,
                          isPrimary: Boolean = false,
                          isRowID: Boolean = false) {

  /**
   * Encodes the [[ColumnMetadata metadata]] into a bit sequence representing the metadata
   * @return a short representing the metadata bits
   */
  def encode: Short = {
    val c = if (isCompressed) COMPRESSED_BIT else 0
    val e = if (isEncrypted) ENCRYPTED_BIT else 0
    val n = if (isNullable) NULLABLE_BIT else 0
    val p = if (isPrimary) PRIMARY_BIT else 0
    val r = if (isRowID) ROW_ID_BIT else 0
    val x = if (isExternal) EXTERNAL_BIT else 0
    val t = `type`.id & TYPE_BITS
    (c | e | n | p | r | t | x).toShort
  }

  override def toString: String =
    s"""|${getClass.getSimpleName}(
        |isCompressed=$isCompressed,
        |isEncrypted=$isEncrypted,
        |isExternal=$isExternal,
        |isNullable=$isNullable,
        |isPrimary=$isPrimary,
        |isRowID=$isRowID,
        |type=${`type`}
        |)""".stripMargin.split("\n").mkString

}

/**
 * Column Metadata Companion
 */
object ColumnMetadata {
  // bit enumerations
  val COMPRESSED_BIT = 0x0200
  val ENCRYPTED_BIT = 0x0100
  val EXTERNAL_BIT = 0x0400
  val NULLABLE_BIT = 0x0080
  val PRIMARY_BIT = 0x0040
  val ROW_ID_BIT = 0x0020
  val TYPE_BITS = 0x000f

  /**
   * Decodes the 16-bit metadata code into [[ColumnMetadata metadata]]
   * @param metadataBits the metadata code
   * @return a new [[ColumnMetadata metadata]]
   */
  def decode(metadataBits: Short): ColumnMetadata = new ColumnMetadata(
    isCompressed = (metadataBits & COMPRESSED_BIT) > 0,
    isEncrypted = (metadataBits & ENCRYPTED_BIT) > 0,
    isExternal = (metadataBits & EXTERNAL_BIT) > 0,
    isNullable = (metadataBits & NULLABLE_BIT) > 0,
    isPrimary = (metadataBits & PRIMARY_BIT) > 0,
    isRowID = (metadataBits & ROW_ID_BIT) > 0,
    `type` = ColumnTypes(metadataBits & TYPE_BITS)
  )

}
