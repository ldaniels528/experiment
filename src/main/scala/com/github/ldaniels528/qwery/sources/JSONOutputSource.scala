package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{OutputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row}
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.compactRender

/**
  * JSON Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONOutputSource(device: OutputDevice, hints: Option[Hints] = None) extends OutputSource {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private var offset = 0L

  override def write(row: Row): Unit = {
    offset += 1
    device.write(Record(compactRender(decompose(Map(row.columns: _*))).getBytes(), offset))
  }

}

