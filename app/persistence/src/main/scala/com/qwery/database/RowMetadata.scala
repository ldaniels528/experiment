package com.qwery.database

import com.qwery.database.RowMetadata._

/**
 * Represents the metadata of a row stored in the database.
 * <pre>
 * ---------------------------------------
 * a - active bit ..... [1000.0000 ~ 0x80]
 * c - compressed bit . [0100.0000 ~ 0x40]
 * e - encrypted bit .. [0010.0000 ~ 0x20]
 * l - locked bit ..... [0001.0000 ~ 0x10]
 * r - reserved bits .. [0000.1111 ~ 0x0f]
 * ---------------------------------------
 * </pre>
 * @param isActive     indicates whether the row is active; meaning not deleted.
 * @param isCompressed indicates whether the row is compressed
 * @param isEncrypted  indicates whether the row is encrypted
 * @param isLocked     indicates whether the row is locked for update
 * @param reservedBits reserved for future use (4 bits)
 */
case class RowMetadata(isActive: Boolean = true,
                       isCompressed: Boolean = false,
                       isEncrypted: Boolean = false,
                       isLocked: Boolean = false,
                       reservedBits: Int = 0) {

  /**
   * Encodes the [[RowMetadata metadata]] into a bit sequence representing the metadata
   * @return a short representing the metadata bits
   */
  def encode: Byte = {
    val a = if (isActive) ACTIVE_BIT else 0
    val c = if (isCompressed) COMPRESSED_BIT else 0
    val e = if (isEncrypted) ENCRYPTED_BIT else 0
    val l = if (isLocked) LOCKED_BIT else 0
    val r = reservedBits & RESERVED_BITS
    (a | c | e | l | r).toByte
  }

  def isDeleted: Boolean = !isActive

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |isActive=$isActive,
        |isCompressed=$isCompressed,
        |isEncrypted=$isEncrypted,
        |isLocked=$isLocked,
        |reservedBits=$reservedBits%02xh
        |)""".stripMargin.split("\n").mkString

}

/**
 * Row MetaData Companion
 */
object RowMetadata {
  // bit enumerations
  val ACTIVE_BIT = 0x80
  val COMPRESSED_BIT = 0x40
  val ENCRYPTED_BIT = 0x20
  val LOCKED_BIT = 0x10
  val RESERVED_BITS = 0x0f

  /**
   * Decodes the 8-bit metadata code into [[RowMetadata metadata]]
   * @param metadataBits the metadata byte
   * @return a new [[RowMetadata metadata]]
   */
  def decode(metadataBits: Byte): RowMetadata = new RowMetadata(
    isActive = (metadataBits & ACTIVE_BIT) > 0,
    isCompressed = (metadataBits & COMPRESSED_BIT) > 0,
    isEncrypted = (metadataBits & ENCRYPTED_BIT) > 0,
    isLocked = (metadataBits & LOCKED_BIT) > 0,
    reservedBits = metadataBits & RESERVED_BITS
  )

}