package com.qwery.database

import java.nio.ByteBuffer
import java.util.UUID

import scala.reflect.runtime.universe._

/**
 * Column Types
 */
object ColumnTypes extends Enumeration {
  type ColumnType = Value

  val ArrayType: ColumnType = Value(0)
  val BigDecimalType: ColumnType = Value(1)
  val BigIntType: ColumnType = Value(2)
  val BinaryType: ColumnType = Value(15)
  val BlobType: ColumnType = Value(3) // can be byte array or a Serializable
  val BooleanType: ColumnType = Value(4)
  val ByteType: ColumnType = Value(5)
  val CharType: ColumnType = Value(6)
  val DateType: ColumnType = Value(7)
  val DoubleType: ColumnType = Value(8)
  val FloatType: ColumnType = Value(9)
  val IntType: ColumnType = Value(10)
  val LongType: ColumnType = Value(11)
  val ShortType: ColumnType = Value(12)
  val StringType: ColumnType = Value(13)
  val UUIDType: ColumnType = Value(14)

  def determineType[A](`class`: Class[A])(implicit tt: TypeTag[A]): ColumnType = {
    `class` match {
      case c if c == classOf[Option[_]] =>
        typeOf[A] match {
          case t if t =:= typeOf[Option[Int]] => IntType
          case t if t =:= typeOf[Option[Long]] | t =:= typeOf[Option[java.lang.Long]] => LongType
          case t if t =:= typeOf[Option[String]] => StringType
          case t => throw new IllegalArgumentException(s"Unrecognized type '${c.getName}:${t.erasure}'")
        }
      case c if c.isArray => ArrayType
      case c if c == classOf[java.math.BigDecimal] => BigDecimalType
      case c if c == classOf[java.math.BigInteger] => BigIntType
      case c if c == classOf[Boolean] | c == classOf[java.lang.Boolean] => BooleanType
      case c if c == classOf[Byte] | c == classOf[java.lang.Byte] => ByteType
      case c if c == classOf[ByteBuffer] => BlobType
      case c if c == classOf[Char] | c == classOf[java.lang.Character] => CharType
      case c if c == classOf[java.util.Date] => DateType
      case c if c == classOf[Double] | c == classOf[java.lang.Double] => DoubleType
      case c if c == classOf[Float] | c == classOf[java.lang.Float] => FloatType
      case c if c == classOf[Int] | c == classOf[java.lang.Integer] => IntType
      case c if c == classOf[Long] | c == classOf[java.lang.Long] => LongType
      case c if c == classOf[Short] | c == classOf[java.lang.Short] => ShortType
      case c if c == classOf[String] => StringType
      case c if c == classOf[UUID] => UUIDType
      case c => throw new IllegalArgumentException(s"Unrecognized class '${c.getName}'")
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
