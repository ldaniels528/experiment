package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.sources.FixedLengthInputSource.FixedField

/**
  * Fixed-length Output Source
  * @author lawrence.daniels@gmail.com
  */
case class FixedLengthOutputSource(device: OutputDevice, fields: Seq[FixedField]) extends OutputSource {
  private var offset = 0L

  override def write(row: Row): Unit = {
    val bytes = fields.foldLeft[StringBuilder](new StringBuilder(row.size * 20)) { case (sb, field) =>
      sb.append(sizeTo(row.get(field.name), field.width))
    }.toString().getBytes()
    offset += 1
    device.write(Record(offset, bytes))
  }

  override def open(): Unit = device.open()

  override def close(): Unit = device.close()

  private def sizeTo(value: Option[Any], width: Int) = {
    value.map(_.toString) match {
      case Some(s) if s.length == width => s
      case Some(s) if s.length > width => s.take(width)
      case Some(s) if s.length < width => s + (" " * (width - s.length))
      case None => " " * width
    }
  }

}
