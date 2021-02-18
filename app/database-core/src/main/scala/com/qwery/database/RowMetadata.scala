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
 * r - replicated bit . [0000.1000 ~ 0x08]
 * x - unused bits .... [0000.0111 ~ 0x07]
 * ---------------------------------------
 * </pre>
 * @param isActive     indicates whether the row is active; meaning not deleted.
 * @param isCompressed indicates whether the row is compressed
 * @param isEncrypted  indicates whether the row is encrypted
 * @param isLocked     indicates whether the row is locked for update
 * @param isReplicated indicates whether the row has been replicated
 */
case class RowMetadata(isActive: Boolean = true,
                       isCompressed: Boolean = false,
                       isEncrypted: Boolean = false,
                       isLocked: Boolean = false,
                       isReplicated: Boolean = false) {

  /**
   * Encodes the [[RowMetadata metadata]] into a bit sequence representing the metadata
   * @return a short representing the metadata bits
   */
  def encode: Byte = {
    val a = if (isActive) ACTIVE_BIT else 0
    val c = if (isCompressed) COMPRESSED_BIT else 0
    val e = if (isEncrypted) ENCRYPTED_BIT else 0
    val l = if (isLocked) LOCKED_BIT else 0
    val r = if (isReplicated) REPLICATED_BIT else 0
    (a | c | e | l | r).toByte
  }

  def isDeleted: Boolean = !isActive

  def isUnlocked: Boolean = !isLocked

  override def toString: String =
    f"""|${getClass.getSimpleName}(
        |isActive=$isActive,
        |isCompressed=$isCompressed,
        |isEncrypted=$isEncrypted,
        |isLocked=$isLocked,
        |isReplicated=$isReplicated
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
  val REPLICATED_BIT = 0x08
  val UNUSED_BITS = 0x07

  // the length of the encoded metadata
  val BYTES_LENGTH = 1

  import spray.json._
  implicit object RowMetadataJsonFormat extends JsonFormat[RowMetadata] {
    override def read(json: JsValue): RowMetadata = json match {
      case JsNumber(value) => RowMetadata.decode(value.toByte)
    }

    override def write(rmd: RowMetadata): JsValue = JsNumber(rmd.encode.toInt)
  }

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
    isReplicated = (metadataBits & REPLICATED_BIT) > 0
  )

}