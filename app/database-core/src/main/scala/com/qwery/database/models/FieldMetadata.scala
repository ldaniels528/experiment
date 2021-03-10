package com.qwery.database
package models

import com.qwery.database.models.FieldMetadata._

/**
 * Represents the metadata of a field stored in the database.
 * <pre>
 * ----------------------------------------
 * a - active bit ...... [0100.0000 ~ 0x40]
 * c - compressed bit .. [0010.0000 ~ 0x20]
 * x - external bit .... [0001.0000 ~ 0x10]
 * ----------------------------------------
 * </pre>
 * @param isActive     indicates whether the field's data is active; meaning not null.
 * @param isCompressed indicates whether the field's data is compressed
 * @param isExternal   indicates whether the field's data is external (e.g. Array or BLOB)
 */
case class FieldMetadata(isActive: Boolean = true,
                         isCompressed: Boolean = false,
                         isExternal: Boolean = false) {

  /**
   * Encodes the [[FieldMetadata metadata]] into a bit sequence representing the metadata
   * @return a byte representing the metadata
   */
  def encode: Byte = {
    val a = if (isActive) ACTIVE_BIT else 0
    val c = if (isCompressed) COMPRESSED_BIT else 0
    val x = if (isExternal) EXTERNAL_BIT else 0
    (a | c | x).toByte
  }

  @inline def isNotNull: Boolean = isActive

  @inline def isNull: Boolean = !isActive

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |isActive=$isActive,
        |isCompressed=$isCompressed,
        |isExternal=$isExternal
        |)""".stripMargin.split("\n").mkString

}

/**
 * Field MetaData Companion
 */
object FieldMetadata {
  // bit enumerations
  val ACTIVE_BIT = 0x40
  val COMPRESSED_BIT = 0x20
  val EXTERNAL_BIT = 0x10

  // the length of the encoded metadata
  val BYTES_LENGTH = 1

  /**
   * Creates new field metadata based on existing column metadata
   * @param column the [[TableColumn column]]
   * @return new [[FieldMetadata field metadata]]
   */
  def apply(column: TableColumn): FieldMetadata = FieldMetadata(
    isCompressed = column.isCompressed,
    isExternal = column.isExternal
  )

  /**
   * Decodes the 8-bit metadata code into [[FieldMetadata metadata]]
   * @param metadataBits the metadata byte
   * @return a new [[FieldMetadata metadata]]
   */
  def decode(metadataBits: Byte): FieldMetadata = new FieldMetadata(
    isActive = (metadataBits & ACTIVE_BIT) > 0,
    isCompressed = (metadataBits & COMPRESSED_BIT) > 0,
    isExternal = (metadataBits & EXTERNAL_BIT) > 0
  )

}