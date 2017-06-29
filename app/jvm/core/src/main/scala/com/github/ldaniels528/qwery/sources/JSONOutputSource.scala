package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{OutputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row}
import com.github.ldaniels528.qwery.util.JSONSupport

/**
  * JSON Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONOutputSource(device: OutputDevice, hints: Option[Hints] = None) extends OutputSource with JSONSupport {
  private var offset = 0L

  override def write(row: Row): Unit = {
    offset += 1
    device.write(Record(toJson(row).getBytes(), offset))
  }

}

