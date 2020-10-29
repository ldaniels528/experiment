package com.qwery.database

import com.qwery.database.FieldMetadata._

/**
 * Represents the metadata of a field stored in the database.
 * <pre>
 * ----------------------------------------
 * a - active bit ...... [1000.0000 ~ 0x80]
 * c - compressed bit .. [0100.0000 ~ 0x40]
 * e - encrypted bit ... [0010.0000 ~ 0x20]
 * x - external bit .... [0001.0000 ~ 0x10]
 * ----------------------------------------
 * </pre>
 * @param isActive     indicates whether the field's data is active; meaning not null.
 * @param isCompressed indicates whether the field's data is compressed
 * @param isEncrypted  indicates whether the field's data is encrypted
 * @param isExternal   indicates whether the field's data is external (e.g. Array or BLOB)
 */
case class FieldMetadata(isActive: Boolean = true,
                         isCompressed: Boolean = false,
                         isEncrypted: Boolean = false,
                         isExternal: Boolean = false) {

  /**
   * Encodes the [[FieldMetadata metadata]] into a bit sequence representing the metadata
   * @return a byte representing the metadata
   */
  def encode: Byte = {
    val a = if (isActive) ACTIVE_BIT else 0
    val c = if (isCompressed) COMPRESSED_BIT else 0
    val e = if (isEncrypted) ENCRYPTED_BIT else 0
    val x = if (isExternal) EXTERNAL_BIT else 0
    (a | c | e | x).toByte
  }

  @inline def isNotNull: Boolean = isActive

  @inline def isNull: Boolean = !isActive

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |isAvailable=$isActive,
        |isCompressed=$isCompressed,
        |isEncrypted=$isEncrypted,
        |isExternal=$isExternal
        |)""".stripMargin.split("\n").mkString

}

/**
 * Field MetaData Companion
 */
object FieldMetadata {
  // bit enumerations
  val ACTIVE_BIT = 0x80
  val COMPRESSED_BIT = 0x40
  val ENCRYPTED_BIT = 0x20
  val EXTERNAL_BIT = 0x10

  // the length of the encoded metadata
  val BYTES_LENGTH = 1

  /**
   * Creates new field metadata based on existing column metadata
   * @param metadata the [[ColumnMetadata column metadata]]
   * @return new [[FieldMetadata field metadata]]
   */
  def apply(metadata: ColumnMetadata): FieldMetadata = FieldMetadata(
    isCompressed = metadata.isCompressed,
    isEncrypted = metadata.isEncrypted,
    isExternal = metadata.`type`.isExternal
  )

  /**
   * Decodes the 8-bit metadata code into [[FieldMetadata metadata]]
   * @param metadataBits the metadata byte
   * @return a new [[FieldMetadata metadata]]
   */
  def decode(metadataBits: Byte): FieldMetadata = new FieldMetadata(
    isActive = (metadataBits & ACTIVE_BIT) > 0,
    isCompressed = (metadataBits & COMPRESSED_BIT) > 0,
    isEncrypted = (metadataBits & ENCRYPTED_BIT) > 0,
    isExternal = (metadataBits & EXTERNAL_BIT) > 0
  )

}