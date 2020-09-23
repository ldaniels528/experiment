package com.qwery.database
package server

import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

/**
 * Represents an OpCode
 */
trait OpCode {

  /**
   * Invokes the opCode
   * @param ctx the implicit [[RuntimeContext runtime context]]
   * @return the option of a [[ByteBuffer]]
   */
  def invoke()(implicit ctx: RuntimeContext): ByteBuffer

  /**
   * @return the binary representation of the opCodes
   */
  def encode: ByteBuffer

}

/**
 * OpCode Companion
 */
object OpCode {
  private val COMMAND_BYTE = 1

  /**
   * Decodes the operational byte code
   * @param buf the [[ByteBuffer]]
   */
  def decode(buf: ByteBuffer): OpCode = {
    val omd = OpCodeMetadata.decode(buf.get())

    import CommandTypes._
    omd.command match {
      // device I/O
      case CREATE_DEVICE => CreateDeviceOp(ref = buf.getDevice, columns = buf.getColumns)
      case DELETE_DEVICE => DeleteDeviceOp(ref = buf.getDevice)
      case DEVICE_SIZE => DeviceSizeOp(ref = buf.getDevice)
      case SHRINK_DEVICE => ShrinkDeviceOp(ref = buf.getDevice, newSize = buf.getRowID)

      // context I/O
      case GET_COLUMNS => GetColumnsOp(ref = buf.getDevice)
      case SET_DEFAULT_DEVICE => SetDefaultDeviceOp(ref = buf.getDevice)

      // row-based I/O
      case READ_BLOCK => ReadBlockOp(ref = buf.getDevice, rowID = buf.getRowID)
      case READ_BYTES => ReadBytesOp(ref = buf.getDevice, rowID = buf.getRowID, numberOfBytes = buf.getShort, offset = buf.getShort)
      case READ_METADATA => ReadMetadataOp(ref = buf.getDevice, rowID = buf.getRowID)
      case SWAP_BLOCK => SwapBlockOp(ref = buf.getDevice, rowID0 = buf.getRowID, rowID1 = buf.getRowID)
      case WRITE_BLOCK => WriteBlockOp(ref = buf.getDevice, rowID = buf.getRowID, bytes = buf.getBytes)
      case WRITE_METADATA => WriteMetadataOp(ref = buf.getDevice, rowID = buf.getRowID, buf.getRowMetadata)

      // unhandled
      case unknown => throw new IllegalStateException(f"Unhandled command $unknown [$omd]")
    }
  }

   case class CreateDeviceOp(ref: DeviceRef, columns: Seq[Column]) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = ???

     override def encode: ByteBuffer = {
       val columnBytesLen = columns.map(_.encode.array().length).sum
       println(s"CreateDeviceOp: columnBytesLen = $columnBytesLen")
       val buf = allocate(COMMAND_BYTE + ref.length + SHORT_BYTES + columnBytesLen)
         .put(OpCodeMetadata(CommandTypes.CREATE_DEVICE).encode)
         .putDevice(ref).putColumns(columns)
       buf.flip().asInstanceOf[ByteBuffer]
     }
  }

  case class DeleteDeviceOp(ref: DeviceRef) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = ???

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length)
        .put(OpCodeMetadata(CommandTypes.DELETE_DEVICE).encode)
        .putDevice(ref)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  case class DeviceSizeOp(ref: DeviceRef) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      allocate(ROW_ID_BYTES).putRowID(ref.toDevice.length)
    }

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length)
        .put(OpCodeMetadata(CommandTypes.DEVICE_SIZE).encode)
        .putDevice(ref)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  case class GetColumnsOp(ref: DeviceRef) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      val device = ref.toDevice
      val columnBytes = device.columns.toArray.flatMap(_.encode.array())
      allocate(columnBytes.length + SHORT_BYTES + INT_BYTES)
        .putShort(device.columns.size.toShort).putInt(columnBytes.length).put(columnBytes)
    }

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length)
        .put(OpCodeMetadata(CommandTypes.GET_COLUMNS).encode)
        .putDevice(ref)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  /**
   * Reads a partial or full block
   * @param ref   the [[DeviceRef]]
   * @param rowID the row ID
   */
  case class ReadBlockOp(ref: DeviceRef, rowID: ROWID) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = ref.toDevice.readBlock(rowID)

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length + INT_BYTES)
        .put(OpCodeMetadata(CommandTypes.READ_BLOCK).encode)
        .putDevice(ref).putRowID(rowID)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  /**
   * Reads a partial or full block
   * @param ref   the [[DeviceRef]]
   * @param rowID the row ID
   */
  case class ReadBytesOp(ref: DeviceRef, rowID: ROWID, numberOfBytes: Short, offset: Short) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      ref.toDevice.readBytes(rowID, numberOfBytes, offset)
    }

   override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length + ROW_ID_BYTES + SHORT_BYTES + SHORT_BYTES)
        .put(OpCodeMetadata(CommandTypes.READ_BYTES).encode)
        .putDevice(ref).putRowID(rowID).putShort(numberOfBytes.toShort).putShort(offset.toShort)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  /**
   * Reads the row metadata for an offset
   * @param ref   the [[DeviceRef]]
   * @param rowID the row ID
   */
  case class ReadMetadataOp(ref: DeviceRef, rowID: ROWID) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      allocate(1).put(ref.toDevice.readRowMetaData(rowID).encode)
    }

   override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length + INT_BYTES)
        .put(OpCodeMetadata(CommandTypes.READ_METADATA).encode).putDevice(ref).putRowID(rowID)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  case class SetDefaultDeviceOp(ref: DeviceRef) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      ctx.defaultDevice = Some(ref.toDevice)
      allocate(0)
    }

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length)
        .put(OpCodeMetadata(CommandTypes.READ_METADATA).encode).putDevice(ref)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  case class ShrinkDeviceOp(ref: DeviceRef, newSize: ROWID) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      ref.toDevice.shrinkTo(newSize)
      allocate(0)
    }

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length + INT_BYTES)
        .put(OpCodeMetadata(CommandTypes.SHRINK_DEVICE).encode)
        .putDevice(ref).putRowID(newSize)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  /**
   * Swaps two partial or full blocks
   * @param ref    the [[DeviceRef]]
   * @param rowID0 the row ID
   * @param rowID1 the row ID
   */
  case class SwapBlockOp(ref: DeviceRef, rowID0: ROWID, rowID1: ROWID) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      ref.toDevice.swap(rowID0, rowID1)
      allocate(0)
    }

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length + INT_BYTES + INT_BYTES)
        .put(OpCodeMetadata(CommandTypes.SWAP_BLOCK).encode)
        .putDevice(ref).putRowID(rowID0).putRowID(rowID1)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  /**
   * Writes a partial or full block
   * @param ref   the [[DeviceRef]]
   * @param rowID the row ID
   * @param bytes   the byte array
   */
  case class WriteBlockOp(ref: DeviceRef, rowID: ROWID, bytes: Array[Byte]) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      ref.toDevice.writeBlock(rowID, wrap(bytes))
      allocate(0)
    }

    override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length + ROW_ID_BYTES + bytes.length + SHORT_BYTES)
        .put(OpCodeMetadata(CommandTypes.WRITE_BLOCK).encode)
        .putDevice(ref).putRowID(rowID).putBytes(bytes)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  /**
   * Writes the row metadata for an offset
   * @param ref      the [[DeviceRef]]
   * @param rowID    the row ID
   * @param metadata the [[RowMetadata]]
   */
  case class WriteMetadataOp(ref: DeviceRef, rowID: ROWID, metadata: RowMetadata) extends OpCode {
    override def invoke()(implicit ctx: RuntimeContext): ByteBuffer = {
      ref.toDevice.writeRowMetaData(rowID, metadata)
      allocate(0)
    }

   override def encode: ByteBuffer = {
      allocate(COMMAND_BYTE + ref.length + INT_BYTES + COMMAND_BYTE)
        .put(OpCodeMetadata(CommandTypes.WRITE_METADATA).encode)
        .putDevice(ref).putRowID(rowID).put(metadata.encode)
        .flip().asInstanceOf[ByteBuffer]
    }
  }

  /**
   * OpCode ByteBuffer
   * @param buf the host [[ByteBuffer]]
   */
  final implicit class OpCodeByteBuffer(val buf: ByteBuffer) extends AnyVal {

    @inline
    def getBytes: Array[Byte] = {
      val count = buf.getShort
      val bytes = new Array[Byte](count)
      buf.get(bytes)
      bytes
    }

    @inline def putBytes(bytes: Array[Byte]): ByteBuffer = buf.putShort(bytes.length.toShort).put(bytes)

    @inline def getDevice: DeviceRef = DeviceRef(name = buf.getString)

    @inline def putDevice(ref: DeviceRef): ByteBuffer = buf.putString(ref.name)

  }

}
