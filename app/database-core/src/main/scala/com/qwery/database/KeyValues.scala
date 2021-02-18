package com.qwery.database

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.device.BlockDevice
import com.qwery.database.models.JsValueConversion

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents a row of key-value pairs
 * @param items the collection of key-value pairs
 */
case class KeyValues(items: (String, Any)*) extends DefaultScope(items: _*) {

  def ++(that: KeyValues): KeyValues = KeyValues(this.toMap ++ that.toMap)

  def filter(f: ((String, Any)) => Boolean): KeyValues = KeyValues(items.filter(f): _*)

  /**
   * Returns the key-value to a binary row
   * @param device the implicit [[BlockDevice]]
   * @return the equivalent [[BinaryRow]]
   */
  def toBinaryRow(implicit device: BlockDevice): BinaryRow = toBinaryRow(rowID = rowID getOrElse device.length)

  /**
   * Returns the key-value to a binary row
   * @param rowID  the unique row ID
   * @param device the implicit [[BlockDevice]]
   * @return the equivalent [[BinaryRow]]
   */
  def toBinaryRow(rowID: ROWID)(implicit device: BlockDevice): BinaryRow = {
    BinaryRow(rowID, metadata = RowMetadata(), fields = device.columns map { column =>
      val buf = allocate(column.maxPhysicalSize)
      val bytes = Codec.encode(column, get(column.name))
      buf.put(bytes)
      buf.flip()
      buf
    })
  }

  /**
   * Returns the key-value to a row buffer
   * @param device the implicit [[BlockDevice]]
   * @return a [[ByteBuffer]]
   */
  def toRowBuffer(implicit device: BlockDevice): ByteBuffer = {
    val buf = allocate(device.recordSize)
    buf.putRowMetadata(RowMetadata())
    device.columns zip device.columnOffsets foreach { case (column, offset) =>
      buf.position(offset)
      buf.put(Codec.encode(column, this.get(column.name)))
    }
    buf.flip()
    buf
  }

}

/**
 * KeyValues Companion
 * @author lawrence.daniels@gmail.com
 */
object KeyValues {

  /**
   * Creates a new row tuple
   * @param mapping the collection of key-value pairs
   * @return a new [[KeyValues]]
   */
  def apply(mapping: Map[String, Any]) = new KeyValues(mapping.toSeq: _*)

  /**
   * Retrieves key-values from the supplied [[ByteBuffer buffer]]
   * @param buf    the supplied [[ByteBuffer buffer]]
   * @param device the implicit [[BlockDevice device]]
   * @return the option of [[KeyValues key-values]]
   */
  def apply(buf: ByteBuffer)(implicit device: BlockDevice): Option[KeyValues] = {
    val rmd = buf.getRowMetadata
    if (!rmd.isActive) None
    else {
      val pairs = device.physicalColumns.zipWithIndex flatMap { case (column, index) =>
        buf.position(device.columnOffsets(index))
        val (_, value_?) = Codec.decode(column, buf)
        value_?.map(column.name -> _)
      }
      Some(KeyValues(pairs: _*))
    }
  }

  @inline
  def isSatisfied(result: => KeyValues, condition: => KeyValues): Boolean = {
    condition.forall { case (name, value) => result.get(name).contains(value) }
  }

  import spray.json._
  implicit object KeyValuesJsonFormat extends JsonFormat[KeyValues] {
    override def read(jsValue: JsValue): KeyValues = jsValue match {
      case js: JsObject => KeyValues(js.fields map { case (name, jsValue) => name -> jsValue.unwrapJSON })
      case x => die(s"Unsupported type $x (${x.getClass.getName})")
    }

    override def write(m: KeyValues): JsValue = {
      JsObject(m.toMap.mapValues {
        case b: Boolean => if (b) JsTrue else JsFalse
        case d: java.util.Date => JsNumber(d.getTime)
        case n: Double => JsNumber(n)
        case n: Float => JsNumber(n)
        case n: Int => JsNumber(n)
        case n: Long => JsNumber(n)
        case n: Number => JsNumber(n.doubleValue())
        case n: Short => JsNumber(n)
        case s: String => JsString(s)
        case v: Any => JsString(v.toString)
      })
    }
  }

}