package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.device.BlockDevice

/**
 * Represents a row of key-value pairs
 * @param items the collection of key-value pairs
 */
case class RowTuple(items: (String, Any)*) {

  def ++(that: RowTuple): RowTuple = RowTuple(this.toMap ++ that.toMap)

  def exists(f: ((String, Any)) => Boolean): Boolean = items.exists(f)

  def forall(f: ((String, Any)) => Boolean): Boolean = items.forall(f)

  def foreach(f: ((String, Any)) => Unit): Unit = items.foreach(f)

  def get(name: String): Option[Any] = toMap.get(name)

  def keys: Seq[String] = items.map(_._1)

  def isEmpty: Boolean = items.isEmpty

  def nonEmpty: Boolean = items.nonEmpty

  /**
   * @param device the implicit [[BlockDevice]]
   * @return the option of a [[BinaryRow]]
   */
  def toBinaryRow(implicit device: BlockDevice): BinaryRow = {
    val id = items.collectFirst { case (name, id: ROWID) if name == ROWID_NAME => id }
    BinaryRow(id getOrElse device.length, buf = toRowBuffer(device))
  }

  def toList: List[(String, Any)] = items.toList

  def toMap: Map[String, Any] = Map(items: _*)

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

  def toSeq: Seq[(String, Any)] = items

  override def toString: String = toMap.toString

  def values: Seq[Any] = items.map(_._2)

}

/**
 * RowTuple Companion
 */
object RowTuple {

  /**
   * Creates a new row tuple
   * @param mapping the collection of key-value pairs
   * @return a new [[RowTuple]]
   */
  def apply(mapping: Map[String, Any]) = new RowTuple(mapping.toSeq: _*)

}