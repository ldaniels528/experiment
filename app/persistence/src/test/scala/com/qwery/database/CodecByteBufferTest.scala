package com.qwery.database

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import org.scalatest.funspec.AnyFunSpec

/**
 * Codec Byte Buffer Test
 */
class CodecByteBufferTest extends AnyFunSpec {

  describe(classOf[CodecByteBuffer].getSimpleName) {

    it("should encode and decode Blob instances") {
      val message = "Testing 1, 2, 3"
      val buf = ByteBuffer.allocate(1024)
      buf.putBlob(message)
      buf.flip()

      val readMessage = buf.getBlob
      assert(message == readMessage)
    }

    it("should encode and decode Date values") {
      val message = new Date()
      val buf = ByteBuffer.allocate(8)
      buf.putDate(message)
      buf.flip()

      val readMessage = buf.getDate
      assert(message == readMessage)
    }

    it("should encode and decode String values") {
      val message = "Hello World"
      val buf = ByteBuffer.allocate(message.length + 2)
      buf.putString(message)
      buf.flip()

      val readMessage = buf.getString
      assert(message == readMessage)
    }

    it("should encode and decode UUID values") {
      val message = UUID.randomUUID()
      val buf = ByteBuffer.allocate(16)
      buf.putUUID(message)
      buf.flip()

      val readMessage = buf.getUUID
      assert(message == readMessage)
    }

  }

}
