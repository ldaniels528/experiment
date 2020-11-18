package com.qwery.database.device

import java.nio.ByteBuffer
import java.util.Base64

import org.scalatest.funspec.AnyFunSpec

class Base36Test extends AnyFunSpec {

  describe("Base36/64") {

    it("should encode a row ID in Base36") {
      val offset = Long.MaxValue
      val rowID = toBase36(offset)
      info(s"offset [$offset] => RowID [$rowID] (${rowID.length} digits)")
      assert(rowID == "1y2p0ij32e8e7")
    }

    it("should encode a row ID in Base64") {
      val offset = Long.MaxValue
      val rowID = toBase64(offset)
      info(s"offset [$offset] => RowID [$rowID] (${rowID.length} digits)")
      assert(rowID == "f/////////8=")
    }

  }

  def toBase36(offset: Long): String = java.lang.Long.toString(offset, 36)

  def toBase64(offset: Long): String = new String(Base64.getEncoder.encode(
    ByteBuffer.allocate(8).putLong(offset).array()
  ))

}
