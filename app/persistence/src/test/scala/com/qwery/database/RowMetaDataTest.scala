package com.qwery.database

import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.util.Random

/**
 * Row Metadata Test Suite
 */
class RowMetaDataTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[RowMetaData].getSimpleName) {

    it("should encode/decode every permutation of metadata") {
      for {
        a <- Seq(true, false)
        c <- Seq(true, false)
        e <- Seq(true, false)
        l <- Seq(true, false)
        r <- 0x0 to 0x0f
      } yield {
        verify(RowMetaData(
          isActive = a,
          isCompressed = c,
          isEncrypted = e,
          isLocked = l,
          reservedBits = r))
      }
    }
  }

  private def verify(md: RowMetaData): Assertion = {
    val code = md.encode
    logger.info(f"$md ~> [$code%02x] ${code.toBinaryString}")
    assert(RowMetaData.decode(code.toByte) == md)
  }

}
