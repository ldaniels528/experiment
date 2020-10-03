package com.qwery.database

import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.qwery.database.ColumnTypes.ColumnType
import com.qwery.database.ColumnTypesTest.CustomBlob
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Column Types Test Suite
 */
class ColumnTypesTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(ColumnTypes.getClass.getSimpleName) {

    it("should detect ArrayType column types") {
      check(value = Array(1, 2, 3), expectedType = ColumnTypes.ArrayType)
    }

    it("should detect BigDecimalType column types") {
      check(value = BigDecimal(123), expectedType = ColumnTypes.BigDecimalType)
      check(value = new java.math.BigDecimal("123.0"), expectedType = ColumnTypes.BigDecimalType)
    }

    it("should detect BigIntType column types") {
      check(value = BigInt(123), expectedType = ColumnTypes.BigIntType)
      check(value = new java.math.BigInteger("123"), expectedType = ColumnTypes.BigIntType)
    }

    it("should detect BinaryType column types") {
      check(value = ByteBuffer.allocate(4).putInt(0xDEADBEEF), expectedType = ColumnTypes.BinaryType)
    }

    it("should detect BlobType column types") {
      check(value = CustomBlob(message = "Hello"), expectedType = ColumnTypes.BlobType)
    }

    it("should detect BooleanType column types") {
      check(value = true, expectedType = ColumnTypes.BooleanType)
    }

    it("should detect ByteType column types") {
      check(value = 123.toByte, expectedType = ColumnTypes.ByteType)
    }

    it("should detect CharType column types") {
      check(value = ' ', expectedType = ColumnTypes.CharType)
    }

    it("should detect DateType column types") {
      check(value = new Date(), expectedType = ColumnTypes.DateType)
    }

    it("should detect DoubleType column types") {
      check(value = 123.0, expectedType = ColumnTypes.DoubleType)
    }

    it("should detect FloatType column types") {
      check(value = 123.0f, expectedType = ColumnTypes.FloatType)
    }

    it("should detect IntType column types") {
      check(value = 123, expectedType = ColumnTypes.IntType)
    }

    it("should detect LongType column types") {
      check(value = 123L, expectedType = ColumnTypes.LongType)
    }

    it("should detect ShortType column types") {
      check(value = 123.toShort, expectedType = ColumnTypes.ShortType)
    }

    it("should detect StringType column types") {
      check(value = "123", expectedType = ColumnTypes.StringType)
      //check(value = Option("123"), expectedType = ColumnTypes.StringType)
    }

    it("should detect UUIDType column types") {
      check(value = UUID.randomUUID(), expectedType = ColumnTypes.UUIDType)
    }

  }

  def check(value: Any, expectedType: ColumnType): Assertion = {
    val columnTypeA = ColumnTypes.determineValueType(value)
    val columnTypeB = ColumnTypes.determineClassType(value.getClass)
    logger.info(s"expected type: $expectedType, class detected type: $columnTypeB, value detected type: $columnTypeA")
    assert(columnTypeA == columnTypeB)
    assert(columnTypeB == expectedType)
  }

}

/**
 * Column Types Test Companion
 */
object ColumnTypesTest {

  case class CustomBlob(message: String)

}
