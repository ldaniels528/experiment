package com.qwery.database

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Field Metadata Test Suite
 */
class FieldMetaDataTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[FieldMetaData].getSimpleName) {

    it("should encode/decode every permutation of metadata") {
      for {
        c <- Seq(true, false)
        e <- Seq(true, false)
        n <- Seq(true, false)
        t <- ColumnTypes.values
      } yield {
        verify(FieldMetaData(
          isCompressed = c,
          isEncrypted = e,
          isNotNull = n,
          `type` = t))
      }
    }
  }

  private def verify(md: FieldMetaData): Assertion = {
    val code = md.encode
    logger.info(f"$md ~> [$code%02x] ${code.toBinaryString}")
    assert(FieldMetaData.decode(code.toByte) == md)
  }

}
