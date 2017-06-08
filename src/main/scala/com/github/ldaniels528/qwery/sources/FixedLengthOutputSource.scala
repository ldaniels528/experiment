package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{OutputDevice, Record}
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.sources.FixedLengthOutputSource.FixedField

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
    device.write(Record(bytes, offset))
  }

  private def sizeTo(value: Option[Any], width: Int) = {
    value.map(_.toString) match {
      case Some(s) if s.length == width => s
      case Some(s) if s.length > width => s.take(width)
      case Some(s) if s.length < width => s + (" " * (width - s.length))
      case None => " " * width
    }
  }

}

/**
  * Fixed-length Output Source Companion
  * @author lawrence.daniels@gmail.com
  */
object FixedLengthOutputSource extends OutputSourceFactory {

  override def findOutputSource(device: OutputDevice, append: Boolean, hints: Option[Hints]): Option[OutputSource] = {
    //if (hints.exists(_.avro.nonEmpty)) Option(FixedLengthOutputSource(device, hints)) else None
    None
  }

  case class FixedField(name: String, width: Int)

}