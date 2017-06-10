package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import com.github.ldaniels528.qwery.sources.FixedLengthInputSource.FixedField

/**
  * Fixed-length Input Source
  * @author lawrence.daniels@gmail.com
  */
case class FixedLengthInputSource(device: InputDevice, fields: Seq[FixedField]) extends InputSource {

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

/**
  * Fixed-length Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object FixedLengthInputSource extends InputSourceFactory {

  override def create(device: InputDevice, hints: Option[Hints]): Option[InputSource] = {
    //if (hints.exists(_.avro.nonEmpty)) Option(FixedLengthInputSource(device, hints)) else None
    None
  }

  case class FixedField(name: String, width: Int)

}
