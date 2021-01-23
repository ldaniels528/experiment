package com.qwery.database

import java.io.{File, FileReader}
import java.nio.ByteBuffer
import java.util.{Date, UUID}

import com.qwery.database.ColumnTypes.{ColumnType, IntType}
import com.qwery.database.ColumnTypesTest.CustomThing
import com.qwery.database.types.ArrayBlock
import com.qwery.database.types.QxAny.RichReader
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
      check(value = ArrayBlock(`type` = IntType, items = Seq(1, 2, 3)), expectedType = ColumnTypes.ArrayType)
    }

    it("should detect BigDecimalType column types") {
      check(value = BigDecimal(123.456), expectedType = ColumnTypes.BigDecimalType)
      check(value = new java.math.BigDecimal("123.456"), expectedType = ColumnTypes.BigDecimalType)
    }

    it("should detect BigIntType column types") {
      check(value = BigInt(123456), expectedType = ColumnTypes.BigIntType)
      check(value = new java.math.BigInteger("123456"), expectedType = ColumnTypes.BigIntType)
    }

    it("should detect BinaryType column types") {
      check(value = ByteBuffer.allocate(4).putInt(0xDEADBEEF), expectedType = ColumnTypes.BinaryType)
    }

    it("should detect BlobType column types") {
      val srcFile = new File("./stocks.csv")
      val blob = new FileReader(srcFile).toBlob
      assert(blob.length() == srcFile.length())
      check(value = blob, expectedType = ColumnTypes.BlobType)
      blob.free()
    }

    it("should detect BooleanType column types") {
      check(value = true, expectedType = ColumnTypes.BooleanType)
    }

    it("should detect ByteType column types") {
      check(value = Byte.MaxValue, expectedType = ColumnTypes.ByteType)
    }

    it("should detect CharType column types") {
      check(value = ' ', expectedType = ColumnTypes.CharType)
    }

    it("should detect ClobType column types") {
      val srcFile = new File("./build.sbt")
      val clob = new FileReader(srcFile).toClob
      assert(clob.length() >= srcFile.length())
      check(value = clob, expectedType = ColumnTypes.ClobType)
      clob.free()
    }

    it("should detect DateType column types") {
      check(value = new Date(), expectedType = ColumnTypes.DateType)
    }

    it("should detect DoubleType column types") {
      check(value = Double.MaxValue, expectedType = ColumnTypes.DoubleType)
    }

    it("should detect FloatType column types") {
      check(value = Float.MaxValue, expectedType = ColumnTypes.FloatType)
    }

    it("should detect IntType column types") {
      check(value = Int.MaxValue, expectedType = ColumnTypes.IntType)
    }

    it("should detect LongType column types") {
      check(value = Long.MaxValue, expectedType = ColumnTypes.LongType)
    }

    it("should detect SerializableType column types") {
      check(value = CustomThing(message = "Hello World"), expectedType = ColumnTypes.SerializableType)
    }

    it("should detect ShortType column types") {
      check(value = Short.MaxValue, expectedType = ColumnTypes.ShortType)
    }

    it("should detect StringType column types") {
      check(value = "Hello World", expectedType = ColumnTypes.StringType)
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

  case class CustomThing(message: String)

}
