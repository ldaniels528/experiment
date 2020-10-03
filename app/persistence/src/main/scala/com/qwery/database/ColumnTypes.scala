package com.qwery.database

import java.nio.ByteBuffer
import java.util.UUID

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

/**
 * Column Types
 */
object ColumnTypes extends Enumeration {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  type ColumnType = Value

  // primitive types
  val BooleanType: ColumnType = Value(0x00) //... 0000
  val ByteType: ColumnType = Value(0x01) // ..... 0001
  val CharType: ColumnType = Value(0x02) // ..... 0010
  val DoubleType: ColumnType = Value(0x03) // ... 0011
  val FloatType: ColumnType = Value(0x04) // .... 0100
  val IntType: ColumnType = Value(0x05) // ...... 0101
  val LongType: ColumnType = Value(0x06) // ..... 0110
  val ShortType: ColumnType = Value(0x07) // .... 0111

  // non-primitive types
  val ArrayType: ColumnType = Value(0x08) // .... 1000
  val BigDecimalType: ColumnType = Value(0x09) // 1001
  val BigIntType: ColumnType = Value(0x0A) // ... 1010
  val BinaryType: ColumnType = Value(0x0B) // ... 1011
  val BlobType: ColumnType = Value(0x0C) // ..... 1100
  val DateType: ColumnType = Value(0x0D) // ..... 1101 (fixed-length)
  val StringType: ColumnType = Value(0x0E) // ... 1110
  val UUIDType: ColumnType = Value(0x0F) // ..... 1111 (fixed-length)

  /**
   * Determines the equivalent column type to the given class
   * @param `class` the given [[Class class]]
   * @return the [[ColumnType]]
   */
  def determineClassType[A](`class`: Class[A])(implicit tt: TypeTag[A]): ColumnType = {
    `class` match {
      case c if c.isArray => ArrayType
      case c if c == classOf[BigDecimal] | c == classOf[java.math.BigDecimal] => BigDecimalType
      case c if c == classOf[BigInt] | c == classOf[java.math.BigInteger] => BigIntType
      case c if c == classOf[Boolean] | c == classOf[java.lang.Boolean] => BooleanType
      case c if c == classOf[Byte] | c == classOf[java.lang.Byte] => ByteType
      case c if c.getName.startsWith("java.nio") & c.getName.contains("Buffer") => BinaryType
      case c if c == classOf[Char] | c == classOf[java.lang.Character] => CharType
      case c if c == classOf[java.util.Date] => DateType
      case c if c == classOf[Double] | c == classOf[java.lang.Double] => DoubleType
      case c if c == classOf[Float] | c == classOf[java.lang.Float] => FloatType
      case c if c == classOf[Int] | c == classOf[java.lang.Integer] => IntType
      case c if c == classOf[Long] | c == classOf[java.lang.Long] => LongType
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
    case t if t =:= typeOf[Some[BigDecimal]] => BigDecimalType
    case t if t =:= typeOf[Some[BigInt]] => BigIntType
    case t if t =:= typeOf[Some[Boolean]] => BooleanType
    //case t if t =:= typeOf[Array[Byte]] => BinaryType
    case t if t =:= typeOf[Some[Byte]] => ByteType
    case t if t =:= typeOf[Some[java.util.Date]] => DateType
    case t if t =:= typeOf[Some[Double]] => DoubleType
    case t if t =:= typeOf[Some[Float]] => FloatType
    case t if t =:= typeOf[Some[Int]] => IntType
    case t if t =:= typeOf[Some[Long]] => LongType
    case t if t =:= typeOf[Some[Short]] => ShortType
    case t if t =:= typeOf[Some[String]] => StringType
    case t if t =:= typeOf[Some[UUID]] => UUIDType
    case t =>
      logger.info(s"Type '$t' is assumed to be a blob")
      BlobType
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
      case _: Array[_] => ArrayType
      case _: BigDecimal => BigDecimalType
      case _: java.math.BigDecimal => BigDecimalType
      case _: BigInt => BigIntType
      case _: java.math.BigInteger => BigIntType
      case _: Boolean => BooleanType
      case _: java.lang.Boolean => BooleanType
      case _: Byte => ByteType
      case _: java.lang.Byte => ByteType
      case _: ByteBuffer => BinaryType
      case _: Char => CharType
      case _: Character => CharType
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
        logger.info(s"Class '${x.getClass}' is assumed to be a blob")
        BlobType
    }
  }

  /**
   * Column Type Extensions
   * @param `type` the [[ColumnType]]
   */
  final implicit class ColumnTypeExtensions(val `type`: ColumnType) extends AnyVal {

    @inline
    def getFixedLength: Option[Int] = `type` match {
      case BigIntType => Some(LONG_BYTES)
      case BooleanType => Some(ONE_BYTE)
      case ByteType => Some(ONE_BYTE)
      case DateType => Some(LONG_BYTES)
      case CharType => Some(SHORT_BYTES)
      case DoubleType => Some(LONG_BYTES)
      case FloatType => Some(INT_BYTES)
      case IntType => Some(INT_BYTES)
      case LongType => Some(LONG_BYTES)
      case ShortType => Some(SHORT_BYTES)
      case UUIDType => Some(2 * LONG_BYTES)
      case _ => None
    }

  }

}
