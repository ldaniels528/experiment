package com.github.ldaniels528.qwery.sources

import FixedLengthOutputSource._
import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{FixedWidth, Hints, Row, Scope}

/**
  * Fixed-length Input Source
  * @author lawrence.daniels@gmail.com
  */
case class FixedLengthInputSource(device: InputDevice, hints: Option[Hints]) extends InputSource {
  private val fields: Seq[FixedWidth] = getFixedFields(hints)

  override def read(scope: Scope): Option[Row] = {
    device.read() map { case Record(bytes, _, _) =>
      val line = new String(bytes)
      var position = 0
      fields.foldLeft[List[(String, String)]](Nil) { case (row, field) =>
        val value = extract(line, position, field.width)
        position += field.width
        row ::: (field.name, value) :: Nil
      }
    }
  }

  private def extract(line: String, position: Int, width: Int) = {
    if (position + width <= line.length) line.substring(position, position + width)
    else if (position < line.length) line.substring(position, line.length)
    else ""
  }

}
