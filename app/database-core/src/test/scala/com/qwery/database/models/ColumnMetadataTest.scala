package com.qwery.database.models

import com.qwery.database.models
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Column Metadata Test Suite
 */
class ColumnMetadataTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[ColumnMetadata].getSimpleName) {

    it("should encode/decode every permutation of metadata") {
      for {
        c <- Seq(true, false)
        e <- Seq(true, false)
        n <- Seq(true, false)
        p <- Seq(true, false)
        r <- Seq(true, false)
        x <- Seq(true, false)
        t <- ColumnTypes.values
      } yield {
        verify(models.ColumnMetadata(
          isCompressed = c,
          isEncrypted = e,
          isExternal = x,
          isNullable = n,
          isPrimary = p,
          isRowID = r,
          `type` = t))
      }
    }
  }

  private def verify(md: ColumnMetadata): Assertion = {
    val code = md.encode
    logger.info(f"$md ~> [$code%04x] ${(code & 0xFFFF).toBinaryString}")
    assert(ColumnMetadata.decode(code) == md)
  }

}
