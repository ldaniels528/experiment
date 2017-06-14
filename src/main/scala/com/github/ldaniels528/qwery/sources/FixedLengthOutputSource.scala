package com.github.ldaniels528.qwery.sources

import FixedLengthOutputSource._
import com.github.ldaniels528.qwery.devices.{OutputDevice, Record}
import com.github.ldaniels528.qwery.ops.{FixedField, Hints, Row, RowEnrichment}

/**
  * Fixed-length Output Source
  * @author lawrence.daniels@gmail.com
  */
case class FixedLengthOutputSource(device: OutputDevice, hints: Option[Hints]) extends OutputSource {
  private val fields: Seq[FixedField] = getFixedFields(hints)
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
object FixedLengthOutputSource {

  def getFixedFields(hints: Option[Hints]): Seq[FixedField] = {
    hints.map(_.getFixedFields).getOrElse(throw new IllegalStateException("No columns defined"))
  }

}
