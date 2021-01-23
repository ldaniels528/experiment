package com.qwery.database

import org.scalatest.funspec.AnyFunSpec

class CompressionTest extends AnyFunSpec {

  describe(classOf[Compression].getSimpleName) {

    it("should compress and decompress data") {
      import Compression._
      val expected = "Hello World"
      val actual = new String(decompressBytes(compressBytes(expected.getBytes)))
      assert(expected == actual)
    }

  }

}
