package com.qwery.database

/**
 * Represents the metadata of a row in the database.
 * <pre>
 * --------------------------------------
 * a - active bit ..... [1000.0000 ~ 0x80]
 * c - compressed bit . [0100.0000 ~ 0x40]
 * e - encrypted bit .. [0010.0000 ~ 0x20]
 * l - locked bit ..... [0001.0000 ~ 0x10]
 * r - reserved bits .. [0000.1111 ~ 0x0f]
 * --------------------------------------
 * </pre>
 * @param isActive     indicates whether the row is active; meaning not deleted.
 * @param isCompressed indicates whether the row is compressed
 * @param isEncrypted  indicates whether the row is encrypted
 * @param isLocked     indicates whether the row is locked for update
 * @param reservedBits reserved for future use (4 bits)
 */
case class RowMetaData(isActive: Boolean = true,
                       isCompressed: Boolean = false,
                       isEncrypted: Boolean = false,
                       isLocked: Boolean = false,
                       reservedBits: Int = 0) {

  def encode: Int = {
    val a = if (isActive) 0x80 else 0
    val c = if (isCompressed) 0x40 else 0
    val e = if (isEncrypted) 0x20 else 0
    val l = if (isLocked) 0x10 else 0
    val r = reservedBits & 0x0f
    a | c | e | l | r
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
object RowMetaData {

  /**
   * Decodes the metadata byte into [[RowMetaData metadata]] instance
   * @param metadataBits the metadata byte
   * @return a new [[RowMetaData metadata]]
   */
  def decode(metadataBits: Byte): RowMetaData = new RowMetaData(
    isActive = (metadataBits & 0x80) > 0,
    isCompressed = (metadataBits & 0x40) > 0,
    isEncrypted = (metadataBits & 0x20) > 0,
    isLocked = (metadataBits & 0x10) > 0,
    reservedBits = metadataBits & 0x0f
  )

}