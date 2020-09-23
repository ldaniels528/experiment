package com.qwery.database.server

/**
 * Command Types
 * <pre>
 * CREATE_DEVICE .. 0000
 * DEFAULT_DEVICE . 0001
 * DELETE_DEVICE .. 0010
 * SHRINK_DEVICE .. 0101
 * GET_COLUMNS .... 0011
 * GET_LENGTH ..... 0100
 * READ_BYTES ..... 0110
 * READ_BLOCK ..... 0111
 * READ_METADATA .. 1000
 * SWAP_BLOCK ..... 1001
 * WRITE_BYTES .... 1010
 * WRITE_BLOCK .... 1011
 * WRITE_METADATA . 1100
 * </pre>
 */
object CommandTypes extends Enumeration {
  type CommandType = Value

  // device I/O
  val CREATE_DEVICE: CommandType = Value(0)
  val DELETE_DEVICE: CommandType = Value(1)
  val SHRINK_DEVICE: CommandType = Value(2)

  // context I/O
  val GET_COLUMNS: CommandType = Value(3)
  val DEVICE_SIZE: CommandType = Value(4)
  val SET_DEFAULT_DEVICE: CommandType = Value(5)

  // row-based I/O
  val READ_BYTES: CommandType = Value(6)
  val READ_BLOCK: CommandType = Value(7)
  val READ_METADATA: CommandType = Value(8)
  val SWAP_BLOCK: CommandType = Value(9)
  val WRITE_BLOCK: CommandType = Value(10)
  val WRITE_METADATA: CommandType = Value(11)

  // reserved
  val NOOP1: CommandType = Value(12)
  val NOOP2: CommandType = Value(13)
  val NOOP3: CommandType = Value(14)
  val NOOP4: CommandType = Value(15)
}