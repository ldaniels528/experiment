package com.qwery.database
package models

import com.qwery.database.types.ArrayBlock
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.sql.{Blob, Clob}
import java.util.UUID
import javax.sql.rowset.serial.{SerialBlob, SerialClob}
import scala.annotation.tailrec
import scala.reflect.runtime.universe._

/**
 * Column Types Enumeration
 */
object ColumnTypes extends Enumeration {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  type ColumnType = Value

  // numeric fixed-length types
  val ByteType: ColumnType = Value(0x00) //........ 00000
  val DoubleType: ColumnType = Value(0x01) //...... 00001
  val FloatType: ColumnType = Value(0x02) //....... 00010
  val IntType: ColumnType = Value(0x03) //......... 00011
  val LongType: ColumnType = Value(0x04) //........ 00100
  val ShortType: ColumnType = Value(0x05) //....... 00101

  // non-numeric fixed-length types
  val BooleanType: ColumnType = Value(0x08) //..... 01000
  val CharType: ColumnType = Value(0x09) //........ 01001
  val DateType: ColumnType = Value(0x0A) //........ 01010
  val UUIDType: ColumnType = Value(0x0B) //........ 01011

  // variable-length types
  val BigDecimalType: ColumnType = Value(0x10) //.. 10000
  val BigIntType: ColumnType = Value(0x11) //...... 10001
  val BinaryType: ColumnType = Value(0x16) //...... 10110
  val StringType: ColumnType = Value(0x17) //...... 10111

  // externally-stored types
  val ArrayType: ColumnType = Value(0x18) //....... 11000 (Array[T] - where T is a non-external type)
  val BlobType: ColumnType = Value(0x19) //........ 11001
  val ClobType: ColumnType = Value(0x1E) //........ 11110
  val SerializableType: ColumnType = Value(0x1F) // 11111

  // potential future types
  // TODO JSONType - JSON could also be handled via StringType
  // TODO MD5Type (fixed-length) - alias for UUID
  // TODO TableType - could also be handled via ArrayType

  /**
   * Determines the equivalent column type to the given class
   * @param `class` the given [[Class class]]
   * @return the [[ColumnType]]
   */
  def determineClassType[A](`class`: Class[A])(implicit tt: TypeTag[A]): ColumnType = {
    `class` match {
      case c if c == classOf[ArrayBlock] | c.isArray => ArrayType
      case c if c == classOf[BigDecimal] | c == classOf[java.math.BigDecimal] => BigDecimalType
      case c if c == classOf[BigInt] | c == classOf[java.math.BigInteger] => BigIntType
      case c if c == classOf[SerialBlob] => BlobType
      case c if c == classOf[Boolean] | c == classOf[java.lang.Boolean] => BooleanType
      case c if c == classOf[Byte] | c == classOf[java.lang.Byte] => ByteType
      case c if c.getName.startsWith("java.nio") & c.getName.contains("Buffer") => BinaryType
      case c if c == classOf[Char] | c == classOf[java.lang.Character] => CharType
      case c if c == classOf[SerialClob] => ClobType
      case c if c == classOf[java.util.Date] | c == classOf[java.sql.Date] | c == classOf[java.sql.Timestamp] => DateType
      case c if c == classOf[Double] | c == classOf[java.lang.Double] => DoubleType
      case c if c == classOf[Float] | c == classOf[java.lang.Float] => FloatType
      case c if c == classOf[Int] | c == classOf[java.lang.Integer] => IntType
      case c if c == classOf[Long] | c == classOf[java.lang.Long] => LongType
      case c if c == classOf[Serializable] => SerializableType
      case c if c == classOf[Short] | c == classOf[java.lang.Short] => ShortType
      case c if c == classOf[String] => StringType
      case c if c == classOf[UUID] => UUIDType
      case _ => determineType[A]
    }
  }

  /**
   * Determines the equivalent column type to the given Scala Type
   * @return the [[ColumnType]]
   */
  def determineType[T: TypeTag]: ColumnType = typeOf[T] match {
    case t if t =:= typeOf[Some[ArrayBlock]] => ArrayType
    case t if t =:= typeOf[Some[BigDecimal]] => BigDecimalType
    case t if t =:= typeOf[Some[BigInt]] => BigIntType
    case t if t =:= typeOf[Some[SerialBlob]] => BlobType
    case t if t =:= typeOf[Some[Boolean]] => BooleanType
    case t if t =:= typeOf[Some[Byte]] => ByteType
    case t if t =:= typeOf[Some[ByteBuffer]] => BinaryType
    case t if t =:= typeOf[Some[SerialClob]] => ClobType
    case t if t =:= typeOf[Some[java.util.Date]] => DateType
    case t if t =:= typeOf[Some[Double]] | t =:= typeOf[Some[java.lang.Double]] => DoubleType
    case t if t =:= typeOf[Some[Float]] | t =:= typeOf[Some[java.lang.Float]] => FloatType
    case t if t =:= typeOf[Some[Int]] | t =:= typeOf[Some[Integer]] => IntType
    case t if t =:= typeOf[Some[Long]] | t =:= typeOf[Some[java.lang.Long]] => LongType
    case t if t =:= typeOf[Some[Short]] | t =:= typeOf[Some[java.lang.Short]] => ShortType
    case t if t =:= typeOf[Some[String]] => StringType
    case t if t =:= typeOf[Some[UUID]] => UUIDType
    case t if t =:= typeOf[Some[Serializable]] => SerializableType
    case _ => SerializableType
  }

  /**
   * Determines the equivalent column type for the given value
   * @param value the given value
   * @return the [[ColumnType]]
   */
  @tailrec
  def determineValueType(value: Any): ColumnType = {
    value match {
      case Some(value) => determineValueType(value)
      case None => BlobType
      case _: ArrayBlock => ArrayType
      case _: BigDecimal => BigDecimalType
      case _: java.math.BigDecimal => BigDecimalType
      case _: BigInt => BigIntType
      case _: java.math.BigInteger => BigIntType
      case _: Blob => BlobType
      case _: Boolean => BooleanType
      case _: java.lang.Boolean => BooleanType
      case _: Byte => ByteType
      case _: java.lang.Byte => ByteType
      case _: ByteBuffer => BinaryType
      case _: Char => CharType
      case _: Character => CharType
      case _: Clob => ClobType
      case _: java.util.Date => DateType
      case _: Double => DoubleType
      case _: java.lang.Double => DoubleType
      case _: Float => FloatType
      case _: java.lang.Float => FloatType
      case _: Int => IntType
      case _: java.lang.Integer => IntType
      case _: Long => LongType
      case _: java.lang.Long => LongType
      case _: Short => ShortType
      case _: java.lang.Short => ShortType
      case _: String => StringType
      case _: UUID => UUIDType
      case x =>
        logger.warn(s"Class '${x.getClass.getName}' is assumed to be a $SerializableType")
        SerializableType
    }
  }

  /**
   * Column type lookup closure
   */
  val lookupColumnType: String => ColumnType = {
    val typeMappings = Map(
      "ARRAY" -> ArrayType,
      "BIGINT" -> BigIntType,
      "BINARY" -> BinaryType,
      "BLOB" -> BlobType,
      "BOOLEAN" -> BooleanType,
      "CHAR" -> StringType,
      "CLOB" -> ClobType,
      "DATE" -> DateType,
      "DATETIME" -> DateType,
      "DECIMAL" -> BigDecimalType,
      "DOUBLE" -> DoubleType,
      "FLOAT" -> FloatType,
      "INT" -> IntType,
      "INTEGER" -> IntType,
      "LONG" -> LongType,
      "OBJECT" -> SerializableType,
      "REAL" -> DoubleType,
      "SHORT" -> ShortType,
      "SMALLINT" -> ShortType,
      "STRING" -> StringType,
      "TEXT" -> ClobType,
      "TIMESTAMP" -> DateType,
      "TINYINT" -> ByteType,
      "UUID" -> UUIDType,
      "VARBINARY" -> BinaryType,
      "VARCHAR" -> StringType
    )
    typeName => typeMappings.getOrElse(typeName.toUpperCase, die(s"Unrecognized data type '$typeName'"))
  }

  /**
   * Column Type Extensions
   * @param `type` the [[ColumnType column type]]
   */
  final implicit class ColumnTypeExtensions(val `type`: ColumnType) extends AnyVal {

    @inline
    def getFixedLength: Option[Int] = `type` match {
      case BooleanType => Some(ONE_BYTE)
      case ByteType => Some(ONE_BYTE)
      case CharType => Some(SHORT_BYTES)
      case DateType => Some(LONG_BYTES)
      case DoubleType => Some(LONG_BYTES)
      case FloatType => Some(INT_BYTES)
      case IntType => Some(INT_BYTES)
      case LongType => Some(LONG_BYTES)
      case ShortType => Some(SHORT_BYTES)
      case UUIDType => Some(2 * LONG_BYTES)
      case _ => None
    }

    @inline
    def isSigned: Boolean = `type` match {
      case BigDecimalType => true
      case BigIntType => true
      case ByteType => true
      case DoubleType => true
      case FloatType => true
      case IntType => true
      case LongType => true
      case ShortType => true
      case _ => false
    }

  }

}
