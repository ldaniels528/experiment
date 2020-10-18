package com.qwery.database

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Field Metadata Test Suite
 */
class FieldMetadataTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[FieldMetadata].getSimpleName) {

    it("should encode/decode every permutation of metadata") {
      for {
        c <- Seq(true, false)
        e <- Seq(true, false)
        n <- Seq(true, false)
        x <- Seq(true, false)
      } yield {
        verify(FieldMetadata(
          isCompressed = c,
          isEncrypted = e,
          isActive = n,
          isExternal = x))
      }
    }
  }

  private def verify(md: FieldMetadata): Assertion = {
    val code = md.encode
    logger.info(f"$md ~> [$code%02x] ${(code & 0xFF).toBinaryString}")
    assert(FieldMetadata.decode(code) == md)
  }

}
