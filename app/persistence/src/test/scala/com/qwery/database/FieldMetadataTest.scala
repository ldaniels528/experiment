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
        t <- ColumnTypes.values
      } yield {
        verify(FieldMetadata(
          isCompressed = c,
          isEncrypted = e,
          isNotNull = n,
          `type` = t))
      }
    }
  }

  private def verify(md: FieldMetadata): Assertion = {
    val code = md.encode
    logger.info(f"$md ~> [$code%02x] ${code.toBinaryString}")
    assert(FieldMetadata.decode(code) == md)
  }

}
