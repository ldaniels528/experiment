package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{OutputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.compactRender

/**
  * JSON Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONOutputSource(device: OutputDevice, hints: Option[Hints] = None) extends OutputSource {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private var offset = 0L

  override def close(): Unit = device.close()

  override def open(scope: Scope): Unit = device.open(scope)

  override def write(row: Row): Unit = {
    offset += 1
    device.write(Record(offset, compactRender(decompose(Map(row: _*))).getBytes()))
  }

}
