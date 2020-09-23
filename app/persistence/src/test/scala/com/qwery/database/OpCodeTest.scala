package com.qwery.database

import com.qwery.database.server.OpCode._
import com.qwery.database.server.{DeviceRef, OpCode}
import org.scalatest.funspec.AnyFunSpec

class OpCodeTest extends AnyFunSpec {

  describe(classOf[OpCode].getSimpleName) {

    it("should encode and decode the CreateDevice opCode") {
      val op = CreateDeviceOp(ref = DeviceRef(name = "test"), columns = Seq(
        Column(name = "name", metadata = ColumnMetadata(`type` = ColumnTypes.StringType), maxSize = Some(32)),
        Column(name = "age", metadata = ColumnMetadata(`type` = ColumnTypes.IntType), maxSize = None)
      ))
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the DeleteDevice opCode") {
      val op = DeleteDeviceOp(ref = DeviceRef(name = "test"))
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the DeviceSize opCode") {
      val op = DeviceSizeOp(ref = DeviceRef(name = "test"))
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the GetColumns opCode") {
      val op = GetColumnsOp(ref = DeviceRef(name = "test"))
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the ReadBlock opCode") {
      val op = ReadBlockOp(ref = DeviceRef(name = "test"), rowID = 3245)
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the ReadBytes opCode") {
      val op = ReadBytesOp(ref = DeviceRef(name = "test"), rowID = 3245, numberOfBytes = 24, offset = 1)
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the ReadMetadata opCode") {
      val op = ReadMetadataOp(ref = DeviceRef(name = "test"), rowID = 8765)
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the ShrinkDevice opCode") {
      val op = ShrinkDeviceOp(ref = DeviceRef(name = "test"), newSize = 5000)
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the SwapBlock opCode") {
      val op = SwapBlockOp(ref = DeviceRef(name = "test"), rowID0 = 32776, rowID1 = 12345)
      assert(OpCode.decode(op.encode) == op)
    }

    it("should encode and decode the WriteBlock opCode") {
      val op0 = WriteBlockOp(ref = DeviceRef(name = "test"), rowID = 3287, bytes = "Hello".getBytes)
      val op1 = OpCode.decode(op0.encode).asInstanceOf[WriteBlockOp]
      assert(op1.ref.name == op0.ref.name)
      assert(op1.rowID == op0.rowID)
      assert(op1.bytes sameElements op0.bytes)
    }

    it("should encode and decode the WriteMetadata opCode") {
      val op = WriteMetadataOp(ref = DeviceRef(name = "test"), rowID = 8765, RowMetadata())
      assert(OpCode.decode(op.encode) == op)
    }

  }

}
