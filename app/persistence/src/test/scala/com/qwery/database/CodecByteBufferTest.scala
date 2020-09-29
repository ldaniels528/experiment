package com.qwery.database

import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.qwery.database.Codec.CodecByteBuffer
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

/**
 * Codec Byte Buffer Test Suite
 */
class CodecByteBufferTest extends AnyFunSpec {

  describe(classOf[CodecByteBuffer].getSimpleName) {

    it("should encode and decode Blob instances") {
      val expected = "Testing 1, 2, 3"
      val buf = ByteBuffer.allocate(26)
      buf.putBlob(expected)
      buf.flip()

      val actual = buf.getBlob
      verify(actual, expected)
    }

    it("should encode and decode BigDecimal instances") {
      val expected = BigDecimal(Math.sqrt(2))
      val buf = ByteBuffer.allocate(11)
      buf.putBigDecimal(expected)
      buf.flip()

      val actual = buf.getBigDecimal
      verify(actual, expected)
    }

    it("should encode and decode BigInteger instances") {
      val expected = BigInteger.valueOf(1e+23.toLong)
      val buf = ByteBuffer.allocate(8)
      buf.putBigInteger(expected)
      buf.flip()

      val actual = buf.getBigInteger
      verify(actual, expected)
    }

    it("should encode and decode Date values") {
      val expected = new Date()
      val buf = ByteBuffer.allocate(8)
      buf.putDate(expected)
      buf.flip()

      val actual = buf.getDate
      verify(actual, expected)
    }

    it("should encode and decode String values") {
      val expected = "Hello World"
      val buf = ByteBuffer.allocate(expected.length + 2)
      buf.putString(expected)
      buf.flip()

      val actual = buf.getString
      verify(actual, expected)
    }

    it("should encode and decode UUID values") {
      val expected = UUID.randomUUID()
      val buf = ByteBuffer.allocate(16)
      buf.putUUID(expected)
      buf.flip()

      val actual = buf.getUUID
      verify(actual, expected)
    }

  }

  def verify[A](actual: A, expected: A): Assertion = {
    info(s"actual:   $actual")
    info(s"expected: $expected")
    assert(actual == expected)
  }

}
