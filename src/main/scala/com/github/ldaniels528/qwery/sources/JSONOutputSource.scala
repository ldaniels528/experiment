package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Row
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.compactRender

/**
  * JSON Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONOutputSource(device: OutputDevice) extends OutputSource {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private var offset = 0L

  override def open(): Unit = device.open()

  override def close(): Unit = device.close()

  override def write(row: Row): Unit = {
    offset += 1
    device.write(Record(offset, compactRender(decompose(Map(row: _*))).getBytes()))
  }

}
