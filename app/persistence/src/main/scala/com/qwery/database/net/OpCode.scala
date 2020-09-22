package com.qwery.database.net

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

import com.qwery.database.net.OpCode.CommandTypes.{CommandType, Value}
import com.qwery.database.net.OpCode.ParamTypes.ParamType
import com.qwery.database.net.OpCode.StorageTypes.StorageType
import com.qwery.database.{BlockDevice, INT_BYTES, ROWID, RowMetadata}

/**
 * Represents an OpCode
 */
trait OpCode {

  def invoke(): Option[ByteBuffer]

}

/**
 * OpCode Companion
 */
object OpCode {

  /**
   * Decodes the operational byte code
   * <pre>
   * 0.000.0000 = 0x00 - read byte
   * 0.000.0001 = 0x01 - read byte <tt>deviceUUID</tt>
   * 0.000.0010 = 0x02 - read byte <tt>offset64</tt>
   * 0.000.0011 = 0x03 - read byte <tt>deviceUUID</tt> <tt>offset64</tt>
   *
   * 0.001.0000 = 0x10 - read bytes <tt>count32</tt>
   * 0.001.0001 = 0x11 - read bytes <tt>deviceUUID</tt> <tt>count32</tt>
   * 0.001.0010 = 0x12 - read bytes <tt>offset64</tt> <tt>count32</tt>
   * 0.001.0011 = 0x13 - read bytes <tt>deviceUUID</tt> <tt>offset64</tt> <tt>count32</tt>
   *
   * 0.010.0000 = 0x20 - read block <tt>deviceUUID</tt> <tt>offset64</tt>
   * 0.110.0000 = 0x60 - read metadata <tt>deviceUUID</tt> <tt>offset64</tt>
   * 1.000.0000 = 0x80 - write byte <tt>deviceUUID</tt> <tt>offset64</tt>
   * 1.000.1000 = 0x80 - swap byte <tt>deviceUUID</tt> <tt>offset64</tt>
   * 1.001.0000 = 0x90 - write bytes <tt>deviceUUID</tt> <tt>offset64</tt> [... bytes]
   * 1.001.1001 = 0x99 - swap bytes <tt>deviceUUID</tt> <tt>count32</tt>
   * 1.010.0000 = 0xA0 - write block <tt>deviceUUID</tt> <tt>offset64</tt> [... bytes]
   * 1.010.1000 = 0xA8 - swap block <tt>deviceUUID</tt> <tt>offset64</tt>
   * 1.110.0000 = 0xE0 - write metadata <tt>deviceUUID</tt> <tt>offset64</tt> <<tt>value8</tt>
   *
   * 1.100.0000 = 0xC0 - close device <tt>deviceUUID</tt>
   * 1.111.0000 = 0xF0 - delete device <tt>deviceUUID</tt>
   * 0.111.0000 = 0x70 - create device <name> <columns> ~> <tt>deviceUUID</tt>
   * 0.100.0000 = 0x40 - open device <name> ~> <tt>deviceUUID</tt>
   *
   * 0.011.0000 = 0x30 - read columns <tt>deviceUUID</tt>
   * 0.101.0000 = 0x50 - read length <tt>deviceUUID</tt>
   * 1.101.0000 = 0xD0 - write length <tt>deviceUUID</tt> <tt>offset64</tt> (shrink)
   * 1.111.1110 = 0xFE - set default device <tt>deviceUUID</tt>
   * 1.111.1111 = 0xFF - set default offset <tt>offset64</tt>
   * </pre>
   * @param buf the [[ByteBuffer]]
   */
  def decode(buf: ByteBuffer): OpCode = {
    val opByte = buf.get().toInt
    val cmdType: CommandType = CommandTypes((opByte & 0xF8) >> 3) // ...... 1111.1000
    val storageType: StorageType = StorageTypes((opByte & 0x06) >> 1) // .. 0000.0110
    val paramType: ParamType = ParamTypes(opByte & 0x01) // ............... 0000.0001
    cmdType match {
      // columns
      case CommandTypes.GET_COLUMNS => GetColumnsOp(device = toDevice(buf))
      // TODO set default offset offset64

      // length / shrink
      case CommandTypes.GET_LENGTH => GetLengthOp(device = toDevice(buf))
      case CommandTypes.SHRINK => ShrinkOp(device = toDevice(buf))

      // devices
      case CommandTypes.CLOSE_DEVICE => ???
      case CommandTypes.CREATE_DEVICE => ???
      case CommandTypes.DEFAULT_DEVICE => ???
      case CommandTypes.DELETE_DEVICE => ???
      case CommandTypes.OPEN_DEVICE => ???
      // TODO set default device deviceUUID

      // read
      case CommandTypes.READ_BYTE => ReadOp(storageType, device = toDevice(buf), offset = toMemoryRef(buf, paramType))
      case CommandTypes.READ_BYTES => ReadOp(storageType, device = toDevice(buf), offset = toMemoryRef(buf, paramType))
      case CommandTypes.READ_BLOCK => ReadOp(storageType, device = toDevice(buf), offset = toMemoryRef(buf, paramType))

      // swap
      case CommandTypes.SWAP_BLOCK => SwapOp(storageType, device = toDevice(buf), offset = toMemoryRef(buf, paramType))

      // write
      case CommandTypes.WRITE_BYTE => WriteOp(storageType, device = toDevice(buf), offset = toMemoryRef(buf, paramType))
      case CommandTypes.WRITE_BYTES => WriteOp(storageType, device = toDevice(buf), offset = toMemoryRef(buf, paramType))
      case CommandTypes.WRITE_BLOCK => WriteOp(storageType, device = toDevice(buf), offset = toMemoryRef(buf, paramType))
      case unknown => throw new IllegalStateException(s"Unhandled command $unknown")
    }
  }

  private def toDevice(buf: ByteBuffer): BlockDevice = ???

  private def toCount32(buf: ByteBuffer): Int = ???

  private def toMemoryRef(buf: ByteBuffer, paramType: ParamType): MemoryRef = ???

  /**
   * Storage types
   * <pre>
   * byte ..... 00
   * bytes .... 01
   * block .... 10
   * metadata . 11
   * </pre>
   */
  object StorageTypes extends Enumeration {
    type StorageType = Value
    val BYTE: StorageType = Value(0)
    val BYTES: StorageType = Value(1)
    val BLOCK: StorageType = Value(2)
    val METADATA: StorageType = Value(3)
  }

  /**
   * Command Types
   * <pre>
   * CREATE_DEVICE .. 0000
   * DEFAULT_DEVICE . 0101
   * OPEN_DEVICE .... 0001
   * CLOSE_DEVICE ... 0010
   * DELETE_DEVICE .. 0011
   * GET_COLUMNS .... 0100
   * GET_LENGTH ..... 0110
   * SHRINK ......... 0111
   * READ ........... 1000
   * WRITE .......... 1001
   * SWAP ........... 1010
   * </pre>
   */
  object CommandTypes extends Enumeration {
    type CommandType = Value
    val CREATE_DEVICE: CommandType = Value(0)
    val OPEN_DEVICE: CommandType = Value(1)
    val CLOSE_DEVICE: CommandType = Value(2)
    val DELETE_DEVICE: CommandType = Value(3)
    val DEFAULT_DEVICE: CommandType = Value(4)

    val GET_COLUMNS: CommandType = Value(5)
    val GET_LENGTH: CommandType = Value(6)
    val SHRINK: CommandType = Value(7)

    val READ_BYTE: CommandType = Value(8)
    val READ_BYTES: CommandType = Value(9)
    val READ_BLOCK: CommandType = Value(10)

    val WRITE_BYTE: CommandType = Value(11)
    val WRITE_BYTES: CommandType = Value(12)
    val WRITE_BLOCK: CommandType = Value(13)

    val SWAP_BLOCK: CommandType = Value(14)
  }

  /*
      val : StorageType = Value(0)
    val BYTES: StorageType = Value(1)
    val BLOCK: StorageType = Value(2)
    val METADATA: StorageType = Value(3)
   */

  /**
   * Parameter Types
   * <pre>
   * device .. 0
   * offset .. 1
   * </pre>
   */
  object ParamTypes extends Enumeration {
    type ParamType = Value
    val DEVICE: ParamType = Value(0)
    val OFFSET: ParamType = Value(1)
  }

  sealed trait MemoryRef {
    def toRowID: ROWID
  }

  case class GetColumnsOp(device: BlockDevice) extends OpCode {
    override def invoke(): Option[ByteBuffer] = {
      device.columns
      val buf = allocate(device.columns.length + INT_BYTES)
        .putInt(device.columns.length)
      Some(buf)
    }
  }

  case class GetLengthOp(device: BlockDevice) extends OpCode {
    override def invoke(): Option[ByteBuffer] = Some(allocate(INT_BYTES).putInt(device.length))
  }

  /**
   * Reads a partial or full block
   * @param storageType the [[StorageType]]
   * @param device      the [[BlockDevice]]
   * @param offset      the [[MemoryRef]]
   */
  case class ReadOp(storageType: StorageType, device: BlockDevice, offset: MemoryRef) extends OpCode {
    override def invoke(): Option[ByteBuffer] = ???
  }

  /**
   * Reads the row metadata for an offset
   * @param device the [[BlockDevice]]
   * @param offset the [[MemoryRef]]
   */
  case class ReadMetadataOp(device: BlockDevice, offset: MemoryRef) extends OpCode {
    override def invoke(): Option[ByteBuffer] = ???
  }

  case class ShrinkOp(device: BlockDevice) extends OpCode {
    override def invoke(): Option[ByteBuffer] = ???
  }

  /**
   * Swaps two partial or full blocks
   * @param storageType the [[StorageType]]
   * @param device      the [[BlockDevice]]
   * @param offset      the [[MemoryRef]]
   */
  case class SwapOp(storageType: StorageType, device: BlockDevice, offset: MemoryRef) extends OpCode {
    override def invoke(): Option[ByteBuffer] = ???
  }

  /**
   * Writes a partial or full block
   * @param storageType the [[StorageType]]
   * @param device      the [[BlockDevice]]
   * @param offset      the [[MemoryRef]]
   */
  case class WriteOp(storageType: StorageType, device: BlockDevice, offset: MemoryRef) extends OpCode {
    override def invoke(): Option[ByteBuffer] = ???
  }

  /**
   * Writes the row metadata for an offset
   * @param device   the [[BlockDevice]]
   * @param offset   the [[MemoryRef]]
   * @param metadata the [[RowMetadata]]
   */
  case class WriteMetadataOp(device: BlockDevice, offset: MemoryRef, metadata: RowMetadata) extends OpCode {
    override def invoke(): Option[ByteBuffer] = {
      device.writeRowMetaData(offset.toRowID, metadata)
      None
    }
  }

}
