package com.qwery.database
package types

import com.qwery.database.models.ColumnTypes._
import com.qwery.database.models.{TableColumn, ColumnTypes, FieldMetadata, JsValueConversion}
import com.qwery.database.util.Codec
import com.qwery.models.expressions.Null
import org.apache.commons.io.IOUtils

import java.io.{ByteArrayOutputStream, InputStream, OutputStreamWriter, Reader}
import java.nio.ByteBuffer
import java.sql.{Blob, Clob}
import java.text.SimpleDateFormat
import java.util.UUID
import javax.sql.rowset.serial.{SerialBlob, SerialClob}
import scala.util.Try

/**
 * Base class for all persistent values
 */
sealed trait QxAny {

  def !==(that: QxAny): Boolean = ! ===(that)

  def ===(that: QxAny): Boolean = (for {a <- this.value; b <- that.value} yield a == b).contains(true)

  def >(that: QxAny): Boolean = (for {a <- this.toDouble; b <- that.toDouble} yield a > b).contains(true)

  def <(that: QxAny): Boolean = (for {a <- this.toDouble; b <- that.toDouble} yield a < b).contains(true)

  def >=(that: QxAny): Boolean = (for {a <- this.toDouble; b <- that.toDouble} yield a >= b).contains(true)

  def <=(that: QxAny): Boolean = (for {a <- this.toDouble; b <- that.toDouble} yield a <= b).contains(true)

  def +(that: QxAny): QxString = QxString(for {a <- this.toQxString.value; b <- that.value} yield a + b)

  def dataType: ColumnType

  /**
   * Encodes this value into a byte array
   * @param column  the given [[TableColumn column]]
   * @return the byte array
   * @see [[Codec.encode]]
   */
  def encode(column: TableColumn): Array[Byte] = Codec.encode(column, value)

  def isNumeric: Boolean = this.isInstanceOf[QxNumber]

  def toByte: Option[Byte] = value.map(_.asInstanceOf[AnyRef]) flatMap {
    case value: java.lang.Boolean => Some(if (value) 1 else 0)
    case value: Number => Some(value.byteValue)
    case value: String => Try(value.toByte).toOption
    case other => throw TypeConversionException(other, ByteType)
  }

  def toChar: Option[Char] = toShort.map(_.toChar)

  def toDate: Option[java.util.Date] = value.map(_.asInstanceOf[AnyRef]) flatMap {
    case value: java.util.Date => Some(value)
    case value: Number => Some(new java.util.Date(value.longValue))
    case value: String => Try(new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ").parse(value)).toOption
    case other => throw TypeConversionException(other, DateType)
  }

  def toDouble: Option[Double] = value.map(_.asInstanceOf[AnyRef]) flatMap {
    case value: java.lang.Boolean => Some(if (value) 1 else 0)
    case value: java.util.Date => Some(value.getTime.toDouble)
    case value: Number => Some(value.doubleValue)
    case value: String => Try(value.toDouble).toOption
    case other => throw TypeConversionException(other, DoubleType)
  }

  def toFloat: Option[Float] = value.map(_.asInstanceOf[AnyRef]) flatMap {
    case value: java.lang.Boolean => Some(if (value) 1 else 0)
    case value: java.util.Date => Some(value.getTime.toFloat)
    case value: Number => Some(value.floatValue)
    case value: String => Try(value.toFloat).toOption
    case other => throw  TypeConversionException(other, FloatType)
  }

  def toInt: Option[Int] = value.map(_.asInstanceOf[AnyRef]) flatMap {
    case value: java.lang.Boolean => Some(if (value) 1 else 0)
    case value: java.util.Date => Some(value.getTime.toInt)
    case value: Number => Some(value.intValue)
    case value: String => Try(value.toInt).toOption
    case other => throw TypeConversionException(other, IntType)
  }

  def toLong: Option[Long] = value.map(_.asInstanceOf[AnyRef]) flatMap {
    case value: java.lang.Boolean => Some(if (value) 1 else 0)
    case value: java.util.Date => Some(value.getTime)
    case value: Number => Some(value.longValue)
    case value: String => Try(value.toLong).toOption
    case other => throw TypeConversionException(other, LongType)
  }

  def toQxString: QxString = QxString(value = value.map(_.toString))

  def toShort: Option[Short] = value.map(_.asInstanceOf[AnyRef]) flatMap {
    case value: java.lang.Boolean => Some(if (value) 1 else 0)
    case value: java.util.Date => Some(value.getTime.toShort)
    case value: Number => Some(value.shortValue)
    case value: String => Try(value.toShort).toOption
    case other => throw TypeConversionException(other, ShortType)
  }

  override def toString: String = value.map(_.toString).getOrElse("")

  def value: Option[Any]

}

/**
 * QxAny Companion
 */
object QxAny {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit object QxAnyJsonFormat extends JsonFormat[QxAny] {
    override def read(value: JsValue): QxAny = QxAny(value.unwrapJSON)

    override def write(value: QxAny): JsValue = value.value.toJson
  }

  def apply(value: Any): QxAny = value match {
    case a: ArrayBlock => QxArray(Some(a))
    case n: BigDecimal => QxBigDecimal(Some(n))
    case n: BigInt => QxBigInt(Some(n))
    case b: Blob => QxBlob(Some(b))
    case b: Boolean => QxBoolean(Some(b))
    case n: Byte => QxByte(Some(n))
    case b: ByteBuffer => QxBinary(Some(b.array()))
    case c: Char => QxChar(Some(c))
    case c: Clob => QxClob(Some(c))
    case d: java.util.Date => QxDate(Some(d))
    case n: Double => QxDouble(Some(n))
    case n: Float => QxFloat(Some(n))
    case n: Int => QxInt(Some(n))
    case n: Long => QxLong(Some(n))
    case null | Null => QxNull
    case o: Option[_] => o.map(apply).getOrElse(QxNull)
    case q: QxAny => q
    case n: Short => QxShort(Some(n))
    case s: String => QxString(Some(s))
    case u: UUID => QxUUID(Some(u))
    case s: Serializable => QxSerializable(Some(s))
    case x => die(s"Unhandled value '$x' (${x.getClass.getName})")
  }

  /**
   * Decodes the buffer as a value based on the given column
   * @param column the given [[TableColumn column]]
   * @param buf    the [[ByteBuffer buffer]]
   * @return a tuple of [[FieldMetadata]] and the [[QxAny value]]
   * @see [[Codec.decode]]
   */
  def decode(column: TableColumn, buf: ByteBuffer): (FieldMetadata, QxAny) = {
    import com.qwery.database.util.Codec.CodecByteBuffer
    implicit val fmd: FieldMetadata = buf.getFieldMetadata
    val value = column.`type` match {
      case ArrayType => QxArray(value = if (fmd.isNull) None else Some(Codec.decodeArray(column, buf)))
      case BigDecimalType => QxBigDecimal(value = if (fmd.isNull) None else Some(buf.getBigDecimal))
      case BigIntType => QxBigInt(value = if (fmd.isNull) None else Some(buf.getBigInteger))
      case BinaryType => QxBinary(value = if (fmd.isNull) None else Some(buf.getBinary))
      case BlobType => QxBlob(value = if (fmd.isNull) None else Some(buf.toBlob))
      case BooleanType => QxBoolean(value = if (fmd.isNull) None else Some(buf.get > 0))
      case ByteType => QxByte(value = if (fmd.isNull) None else Some(buf.get))
      case CharType => QxChar(value = if (fmd.isNull) None else Some(buf.getChar))
      case ClobType => QxClob(value = if (fmd.isNull) None else Some(buf.toClob))
      case DateType => QxDate(value = if (fmd.isNull) None else Some(buf.getDate))
      case DoubleType => QxDouble(value = if (fmd.isNull) None else Some(buf.getDouble))
      case FloatType => QxFloat(value = if (fmd.isNull) None else Some(buf.getFloat))
      case IntType => QxInt(value = if (fmd.isNull) None else Some(buf.getInt))
      case LongType => QxLong(value = if (fmd.isNull) None else Some(buf.getLong))
      case SerializableType => QxSerializable(value = if (fmd.isNull) None else Some(buf.getSerializable))
      case ShortType => QxShort(value = if (fmd.isNull) None else Some(buf.getShort))
      case StringType if column.isEnum => QxString(value = if (fmd.isNull) None else Some(column.enumValues(buf.getShort)))
      case StringType => QxString(value = if (fmd.isNull) None else Some(buf.getText))
      case UUIDType => QxUUID(value = if (fmd.isNull) None else Some(buf.getUUID))
      case unknown => throw TypeConversionException(buf, unknown)
    }
    (fmd, value)
  }

  def unapply(instance: QxAny): Option[Option[Any]] = Some(instance.value)

  final implicit def booleanToQx(value: Boolean): QxBoolean = if (value) QxTrue else QxFalse

  final implicit def booleanToQx(value: Option[Boolean]): QxBoolean = if (value.contains(true)) QxTrue else QxFalse

  final implicit def byteToQx(value: Byte): QxByte = QxByte(Some(value))

  final implicit def byteToQx(value: Option[Byte]): QxByte = QxByte(value)

  final implicit def charToQx(value: Char): QxChar = QxChar(Some(value))

  final implicit def charToQx(value: Option[Char]): QxChar = QxChar(value)

  final implicit def doubleToQx(value: Double): QxDouble = QxDouble(Some(value))

  final implicit def doubleToQx(value: Option[Double]): QxDouble = QxDouble(value)

  final implicit def floatToQx(value: Float): QxFloat = QxFloat(Some(value))

  final implicit def floatToQx(value: Option[Float]): QxFloat = QxFloat(value)

  final implicit def intToQx(value: Int): QxInt = QxInt(Some(value))

  final implicit def intToQx(value: Option[Int]): QxInt = QxInt(value)

  final implicit def longToQx(value: Long): QxLong = QxLong(Some(value))

  final implicit def longToQx(value: Option[Long]): QxLong = QxLong(value)

  final implicit def shortToQx(value: Short): QxShort = QxShort(Some(value))

  final implicit def shortToQx(value: Option[Short]): QxShort = QxShort(value)

  final implicit def stringToQx(value: String): QxString = QxString(Some(value))

  final implicit def stringToQx(value: Option[String]): QxString = QxString(value)

  final implicit def uuidStringToQx(value: String): QxUUID = QxUUID(Some(UUID.fromString(value)))

  final implicit def uuidStringToQx(value: Option[String]): QxUUID = QxUUID(value.map(UUID.fromString))

  final implicit def uuidToQx(value: UUID): QxUUID = QxUUID(Some(value))

  final implicit def uuidToQx(value: Option[UUID]): QxUUID = QxUUID(value)

  final implicit class RichByteBuffer(val buf: ByteBuffer) extends AnyVal {
    @inline def toBlob: Blob = new SerialBlob(buf.array())

    @inline def toClob: Clob = new SerialClob(new String(buf.array()).toCharArray)
  }

  final implicit class QxRichDouble(val value: Double) extends AnyVal {
    @inline def toQxDouble: QxDouble = value: QxDouble

    @inline def toQxFloat: QxFloat = value.toFloat: QxFloat
  }

  final implicit class QxRichInt(val value: Long) extends AnyVal {
    @inline def toQxByte: QxByte = value.toByte: QxByte

    @inline def toQxInt: QxInt = value.toInt: QxInt

    @inline def toQxLong: QxLong = value: QxLong

    @inline def toQxShort: QxShort = value.toShort: QxShort
  }

  final implicit class RichInputStream(val in: InputStream) extends AnyVal {
    def toBlob: Blob = {
      val out = new ByteArrayOutputStream()
      IOUtils.copyLarge(in, out)
      new SerialBlob(out.toByteArray)
    }

    def toBlob(length: Long): Blob = {
      val out = new ByteArrayOutputStream()
      IOUtils.copyLarge(in, out)
      val blob =  new SerialBlob(out.toByteArray)
      blob.truncate(length)
      blob
    }
  }

  final implicit class RichReader(val in: Reader) extends AnyVal {
    def toBlob: Blob = {
      val mem = new ByteArrayOutputStream()
      val out = new OutputStreamWriter(mem)
      IOUtils.copy(in, out)
      out.flush()
      new SerialBlob(mem.toByteArray)
    }

    def toBlob(length: Long): Blob = {
      val mem = new ByteArrayOutputStream()
      val out = new OutputStreamWriter(mem)
      IOUtils.copy(in, out)
      out.flush()
      val blob = new SerialBlob(mem.toByteArray)
      blob.truncate(length)
      blob
    }

    def toClob: Clob = {
      val mem = new ByteArrayOutputStream()
      val out = new OutputStreamWriter(mem)
      IOUtils.copy(in, out)
      out.flush()
      new SerialClob(mem.toByteArray.map(_.toChar))
    }

    def toClob(length: Long): Clob = {
      val mem = new ByteArrayOutputStream()
      val out = new OutputStreamWriter(mem)
      IOUtils.copy(in, out)
      out.flush()
      val clob = new SerialClob(mem.toByteArray.map(_.toChar))
      clob.truncate(length)
      clob
    }
  }

}

case class QxArray(value: Option[ArrayBlock]) extends QxAny {
  def this(value: ArrayBlock) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.ArrayType
}

case class QxBigDecimal(value: Option[BigDecimal]) extends QxNumber {
  def this(value: BigDecimal) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.BigDecimalType
}

case class QxBigInt(value: Option[BigInt]) extends QxNumber {
  def this(value: BigInt) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.BigDecimalType
}

case class QxBinary(value: Option[Array[Byte]]) extends QxAny {
  def this(value: Array[Byte]) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.BinaryType
}

case class QxBlob(value: Option[Blob]) extends QxAny {
  def this(value: Blob) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.BlobType
}

class QxBoolean(val value: Option[Boolean]) extends QxAny {
  override def dataType: ColumnType = ColumnTypes.BooleanType
}

case object QxFalse extends QxBoolean(Some(false))

case object QxTrue extends QxBoolean(Some(true))

object QxBoolean {
  def apply(value: Boolean): QxBoolean = if (value) QxTrue else QxFalse

  def apply(value: Option[Boolean]): QxBoolean = if (value.contains(true)) QxTrue else QxFalse

  def unapply(bool: QxBoolean): Option[Option[Boolean]] = Some(bool.value)
}

case class QxByte(value: Option[Byte]) extends QxNumber {
  def this(value: Byte) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.ByteType
}

case class QxChar(value: Option[Char]) extends QxAny {

  def +(that: QxChar): QxChar = for {a <- this.toChar; b <- that.toChar} yield (a + b).toChar

  def -(that: QxChar): QxChar = for {a <- this.toChar; b <- that.toChar} yield (a - b).toChar

  def *(that: QxChar): QxChar = for {a <- this.toChar; b <- that.toChar} yield (a * b).toChar

  def /(that: QxChar): QxChar = for {a <- this.toChar; b <- that.toChar} yield (a / b).toChar

  def %(that: QxChar): QxChar = for {a <- this.toChar; b <- that.toChar} yield (a % b).toChar

  override def dataType: ColumnType = ColumnTypes.CharType

}

case class QxClob(value: Option[Clob]) extends QxAny {
  def this(value: Clob) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.ClobType
}

case class QxDate(value: Option[java.util.Date]) extends QxNumber {
  def this(value: java.util.Date) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.DateType
}

case class QxDouble(value: Option[Double]) extends QxNumber {
  def this(value: Double) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.DoubleType
}

case class QxFloat(value: Option[Float]) extends QxNumber {
  def this(value: Float) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.FloatType
}

case class QxInt(value: Option[Int]) extends QxNumber {
  def this(value: Int) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.IntType
}

case class QxLong(value: Option[Long]) extends QxNumber {
  def this(value: Long) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.LongType
}

object QxNull extends QxAny {
  override def dataType: ColumnType = ColumnTypes.StringType // TODO create a special type for NULL

  override def value: Option[Any] = None

  override def toString: String = "null"
}

trait QxNumber extends QxAny {

  def +(that: QxNumber): QxNumber = (this, that) match {
    // handle Date values
    case (aa: QxDate, bb: QxNumber) => QxDate(for {a <- aa.value.map(_.getTime); b <- bb.toLong} yield new java.util.Date(a + b))
    case (aa: QxNumber, bb: QxDate) => bb + aa
    // handle Double values
    case (aa: QxDouble, bb: QxNumber) => QxDouble(for {a <- aa.value; b <- bb.toDouble} yield a + b)
    case (aa: QxNumber, bb: QxDouble) => bb + aa
    // handle Float values
    case (aa: QxFloat, bb: QxNumber) => QxFloat(for {a <- aa.value; b <- bb.toFloat} yield a + b)
    case (aa: QxNumber, bb: QxFloat) => bb + aa
    // handle Long values
    case (aa: QxLong, bb: QxNumber) => QxLong(for {a <- aa.value; b <- bb.toLong} yield a + b)
    case (aa: QxNumber, bb: QxLong) => bb + aa
    // handle Int values
    case (aa: QxInt, bb: QxNumber) => QxInt(for {a <- aa.value; b <- bb.toInt} yield a + b)
    case (aa: QxNumber, bb: QxInt) => bb + aa
    // handle Short values
    case (aa: QxShort, bb: QxNumber) => QxShort(for {a <- aa.value; b <- bb.toShort} yield (a + b).toShort)
    case (aa: QxNumber, bb: QxShort) => bb + aa
    // handle Byte values
    case (aa: QxByte, bb: QxNumber) => QxByte(for {a <- aa.value; b <- bb.toByte} yield (a + b).toByte)
    case (aa: QxNumber, bb: QxShort) => bb + aa
    // anything else ...
    case (aa, bb) => die(s"Cannot add '$bb' to '$aa'")
  }

  def -(that: QxNumber): QxNumber = (this, that) match {
    // handle Date values
    case (aa: QxDate, bb: QxNumber) => QxDate(for {a <- aa.value.map(_.getTime); b <- bb.toLong} yield new java.util.Date(a - b))
    case (aa: QxNumber, bb: QxDate) => bb - aa
    // handle Double values
    case (aa: QxDouble, bb: QxNumber) => QxDouble(for {a <- aa.value; b <- bb.toDouble} yield a - b)
    case (aa: QxNumber, bb: QxDouble) => bb - aa
    // handle Float values
    case (aa: QxFloat, bb: QxNumber) => QxFloat(for {a <- aa.value; b <- bb.toFloat} yield a - b)
    case (aa: QxNumber, bb: QxFloat) => bb - aa
    // handle Long values
    case (aa: QxLong, bb: QxNumber) => QxLong(for {a <- aa.value; b <- bb.toLong} yield a - b)
    case (aa: QxNumber, bb: QxLong) => bb - aa
    // handle Int values
    case (aa: QxInt, bb: QxNumber) => QxInt(for {a <- aa.value; b <- bb.toInt} yield a - b)
    case (aa: QxNumber, bb: QxInt) => bb - aa
    // handle Short values
    case (aa: QxShort, bb: QxNumber) => QxShort(for {a <- aa.value; b <- bb.toShort} yield (a - b).toShort)
    case (aa: QxNumber, bb: QxShort) => bb - aa
    // handle Byte values
    case (aa: QxByte, bb: QxNumber) => QxByte(for {a <- aa.value; b <- bb.toByte} yield (a - b).toByte)
    case (aa: QxNumber, bb: QxShort) => bb - aa
    // anything else ...
    case (aa, bb) => die(s"Cannot subtract '$bb' from '$aa'")
  }

  def *(that: QxNumber): QxNumber = (this, that) match {
    // handle Date values
    case (aa: QxDate, bb: QxNumber) => QxDate(for {a <- aa.value.map(_.getTime); b <- bb.toLong} yield new java.util.Date(a * b))
    case (aa: QxNumber, bb: QxDate) => bb * aa
    // handle Double values
    case (aa: QxDouble, bb: QxNumber) => QxDouble(for {a <- aa.value; b <- bb.toDouble} yield a * b)
    case (aa: QxNumber, bb: QxDouble) => bb * aa
    // handle Float values
    case (aa: QxFloat, bb: QxNumber) => QxFloat(for {a <- aa.value; b <- bb.toFloat} yield a * b)
    case (aa: QxNumber, bb: QxFloat) => bb * aa
    // handle Long values
    case (aa: QxLong, bb: QxNumber) => QxLong(for {a <- aa.value; b <- bb.toLong} yield a * b)
    case (aa: QxNumber, bb: QxLong) => bb * aa
    // handle Int values
    case (aa: QxInt, bb: QxNumber) => QxInt(for {a <- aa.value; b <- bb.toInt} yield a * b)
    case (aa: QxNumber, bb: QxInt) => bb * aa
    // handle Short values
    case (aa: QxShort, bb: QxNumber) => QxShort(for {a <- aa.value; b <- bb.toShort} yield (a * b).toShort)
    case (aa: QxNumber, bb: QxShort) => bb * aa
    // handle Byte values
    case (aa: QxByte, bb: QxNumber) => QxByte(for {a <- aa.value; b <- bb.toByte} yield (a * b).toByte)
    case (aa: QxNumber, bb: QxShort) => bb * aa
    // anything else ...
    case (aa, bb) => die(s"Cannot multiply '$bb' by '$aa'")
  }

  def /(that: QxNumber): QxNumber = (this, that) match {
    // handle Date values
    case (aa: QxDate, bb: QxNumber) => QxDate(for {a <- aa.value.map(_.getTime); b <- bb.toLong} yield new java.util.Date(a / b))
    case (aa: QxNumber, bb: QxDate) => bb / aa
    // handle Double values
    case (aa: QxDouble, bb: QxNumber) => QxDouble(for {a <- aa.value; b <- bb.toDouble} yield a / b)
    case (aa: QxNumber, bb: QxDouble) => bb / aa
    // handle Float values
    case (aa: QxFloat, bb: QxNumber) => QxFloat(for {a <- aa.value; b <- bb.toFloat} yield a / b)
    case (aa: QxNumber, bb: QxFloat) => bb / aa
    // handle Long values
    case (aa: QxLong, bb: QxNumber) => QxLong(for {a <- aa.value; b <- bb.toLong} yield a / b)
    case (aa: QxNumber, bb: QxLong) => bb / aa
    // handle Int values
    case (aa: QxInt, bb: QxNumber) => QxInt(for {a <- aa.value; b <- bb.toInt} yield a / b)
    case (aa: QxNumber, bb: QxInt) => bb / aa
    // handle Short values
    case (aa: QxShort, bb: QxNumber) => QxShort(for {a <- aa.value; b <- bb.toShort} yield (a / b).toShort)
    case (aa: QxNumber, bb: QxShort) => bb / aa
    // handle Byte values
    case (aa: QxByte, bb: QxNumber) => QxByte(for {a <- aa.value; b <- bb.toByte} yield (a / b).toByte)
    case (aa: QxNumber, bb: QxShort) => bb / aa
    // anything else ...
    case (aa, bb) => die(s"Cannot divide '$bb' by '$aa'")
  }

  def %(that: QxNumber): QxNumber = (this, that) match {
    // handle Date values
    case (aa: QxDate, bb: QxNumber) => QxDate(for {a <- aa.value.map(_.getTime); b <- bb.toLong} yield new java.util.Date(a % b))
    case (aa: QxNumber, bb: QxDate) => bb % aa
    // handle Double values
    case (aa: QxDouble, bb: QxNumber) => QxDouble(for {a <- aa.value; b <- bb.toDouble} yield a % b)
    case (aa: QxNumber, bb: QxDouble) => bb % aa
    // handle Float values
    case (aa: QxFloat, bb: QxNumber) => QxFloat(for {a <- aa.value; b <- bb.toFloat} yield a % b)
    case (aa: QxNumber, bb: QxFloat) => bb % aa
    // handle Long values
    case (aa: QxLong, bb: QxNumber) => QxLong(for {a <- aa.value; b <- bb.toLong} yield a % b)
    case (aa: QxNumber, bb: QxLong) => bb % aa
    // handle Int values
    case (aa: QxInt, bb: QxNumber) => QxInt(for {a <- aa.value; b <- bb.toInt} yield a % b)
    case (aa: QxNumber, bb: QxInt) => bb % aa
    // handle Short values
    case (aa: QxShort, bb: QxNumber) => QxShort(for {a <- aa.value; b <- bb.toShort} yield (a % b).toShort)
    case (aa: QxNumber, bb: QxShort) => bb % aa
    // handle Byte values
    case (aa: QxByte, bb: QxNumber) => QxByte(for {a <- aa.value; b <- bb.toByte} yield (a % b).toByte)
    case (aa: QxNumber, bb: QxShort) => bb % aa
    // anything else ...
    case (aa, bb) => die(s"Cannot multiply '$bb' by '$aa'")
  }

}

object QxNumber {

  def unapply(number: QxNumber): Option[Option[Double]] = Option(number match {
    case QxBigDecimal(value) => value.map(_.toDouble)
    case QxBigInt(value) => value.map(_.toDouble)
    case QxByte(value) => value.map(_.toDouble)
    case QxDate(value) => value.map(_.getTime.toDouble)
    case QxDouble(value) => value
    case QxFloat(value) => value.map(_.toDouble)
    case QxInt(value) => value.map(_.toDouble)
    case QxLong(value) => value.map(_.toDouble)
    case QxShort(value) => value.map(_.toDouble)
  })

}

case class QxSerializable(value: Option[Serializable]) extends QxAny {

  def this(value: Serializable) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.SerializableType

  override def encode(column: TableColumn): Array[Byte] = Codec.encodeObject(value)
}

case class QxShort(value: Option[Short]) extends QxNumber {
  def this(value: Short) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.ShortType
}

case class QxString(value: Option[String]) extends QxAny {

  def this(value: String) = this(Some(value))

  def +(that: QxString): QxString = QxString(for {s0 <- value; s1 <- that.value} yield s0 + s1)

  def *(that: QxInt): QxString = QxString(for {s <- value; n <- that.value} yield s * n)

  def contains(string: String): Boolean = value.contains(string)

  def contains(string: QxString): Boolean = {
    (for {
      text <- value
      subtext <- string.value
    } yield text.contains(subtext)).contains(true)
  }

  def matches(regex: String): Boolean = value.exists(_.matches(regex))

  def matches(regex: QxString): Boolean = {
    (for {
      text <- value
      regexString <- regex.value
    } yield text.matches(regexString)).contains(true)
  }

  override def dataType: ColumnType = ColumnTypes.StringType
}

case class QxUUID(value: Option[UUID]) extends QxAny {

  def this(value: String) = this(Some(UUID.fromString(value)))

  def this(value: UUID) = this(Some(value))

  override def dataType: ColumnType = ColumnTypes.UUIDType
}
