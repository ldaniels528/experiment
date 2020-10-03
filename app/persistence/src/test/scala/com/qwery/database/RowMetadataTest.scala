package com.qwery.database

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Row Metadata Test Suite
 */
class RowMetadataTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[RowMetadata].getSimpleName) {

    it("should encode/decode every permutation of metadata") {
      for {
        a <- Seq(true, false)
        c <- Seq(true, false)
        e <- Seq(true, false)
        l <- Seq(true, false)
        r <- Seq(true, false)
        u <- 0x00 to 0x07
      } yield {
        verify(RowMetadata(
          isActive = a,
          isCompressed = c,
          isEncrypted = e,
          isLocked = l,
          isReplicated = r,
          unusedBits = u))
      }
    }
  }

  private def verify(md: RowMetadata): Assertion = {
    val code = md.encode
    logger.info(f"$md ~> [$code%02x] ${code.toBinaryString}")
    assert(RowMetadata.decode(code) == md)
  }

}
