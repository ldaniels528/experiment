package com.qwery.database.util

import org.scalatest.funspec.AnyFunSpec

import java.util.Base64

class CompressionTest extends AnyFunSpec {

  describe(classOf[Compression].getSimpleName) {

    it("should compress and decompress data via GZIP") {
      import Compression._
      val expected = "Hello!!World!!" * 100
      val compressed = compressGZIP(expected.getBytes)
      val actual = new String(decompressGZIP(compressed))
      info(s"expected:   ${expected.take(32)}... [${expected.length}]")
      info(s"compressed: ${new String(Base64.getEncoder.encode(compressed).take(32))}... [${compressed.length}]")
      info(s"actual:     ${actual.take(32)}... [${actual.length}]")
      assert(expected == actual)
    }

    it("should compress and decompress data via Snappy") {
      import Compression._
      val expected = "Hello!!World!!" * 100
      val compressed = compressSnappy(expected.getBytes)
      val actual = new String(decompressSnappy(compressed))
      info(s"expected:   ${expected.take(32)}... [${expected.length}]")
      info(s"compressed: ${new String(Base64.getEncoder.encode(compressed).take(32))}... [${compressed.length}]")
      info(s"actual:     ${actual.take(32)}... [${actual.length}]")
      assert(expected == actual)
    }

  }

}
