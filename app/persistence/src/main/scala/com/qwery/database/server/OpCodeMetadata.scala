package com.qwery.database.server

import com.qwery.database.server.CommandTypes.CommandType
import com.qwery.database.server.OpCodeMetadata.{COMMAND_BITS, DIRECT_DEVICE_BIT}

/**
 * OpCode Metadata
 * <pre>
 * ----------------------------------------
 * d - direct device bit [1000.0000 ~ 0x80]
 * c - command bits      [0000.1111 ~ 0x0F]
 * ----------------------------------------
 * </pre>
 * @param command        the [[CommandType]]
 * @param isDirectDevice indicates whether a device will be embedded; else the default will be used
 */
case class OpCodeMetadata(command: CommandType, isDirectDevice: Boolean = false) {

  /**
   * Encodes the [[OpCodeMetadata metadata]] into a bit sequence representing the metadata
   * @return a byte representing the metadata
   */
  def encode: Byte = {
    val d = if (isDirectDevice) DIRECT_DEVICE_BIT else 0
    val c = command.id & COMMAND_BITS
    (d | c).toByte
  }

}

/**
 * OpCode Metadata Companion
 */
object OpCodeMetadata {
  // bit enumerations
  val DIRECT_DEVICE_BIT = 0x80
  val COMMAND_BITS = 0x0F

  /**
   * Decodes the 8-bit metadata code into [[OpCodeMetadata metadata]]
   * @param metadataBits the metadata byte
   * @return a new [[OpCodeMetadata metadata]]
   */
  def decode(metadataBits: Byte): OpCodeMetadata = new OpCodeMetadata(
    command = CommandTypes(metadataBits & COMMAND_BITS),
    isDirectDevice = (metadataBits & DIRECT_DEVICE_BIT) > 0
  )

}