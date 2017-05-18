package com.github.ldaniels528.qwery.formats

import com.github.ldaniels528.qwery.formats.FixedLengthFormat.FixedField
import com.github.ldaniels528.qwery.ops._

/**
  * Fixed-length Format
  * @author lawrence.daniels@gmail.com
  */
case class FixedLengthFormat(fields: Seq[FixedField]) extends TextFormat {

  override def fromText(line: String): Row = {
    var position = 0
    fields.foldLeft[List[(String, String)]](Nil) { case (row, field) =>
      val value = extract(line, position, field.width)
      position += field.width
      row ::: (field.name, value) :: Nil
    }
  }

  override def toText(row: Row): Seq[String] = {
    val sbc = fields.foldLeft[StringBuilder](new StringBuilder()) { case (sb, field) =>
      sb.append(sizeTo(row.get(field.name), field.width))
    }
    Seq(sbc.toString())
  }

  private def extract(line: String, position: Int, width: Int) = {
    if (position + width <= line.length) line.substring(position, position + width)
    else if (position < line.length) line.substring(position, line.length)
    else ""
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
  * Fixed-length Format Companion
  * @author lawrence.daniels@gmail.com
  */
object FixedLengthFormat {

  case class FixedField(name: String, width: Int)

}