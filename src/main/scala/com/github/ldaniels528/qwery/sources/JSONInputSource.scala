package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Hints, Row}
import net.liftweb.json.{JObject, parse}

/**
  * JSON Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONInputSource(device: InputDevice, hints: Option[Hints] = None) extends InputSource {

  override def read(): Option[Row] = {
    device.read() match {
      case Some(Record(_, bytes)) =>
        parse(new String(bytes)) match {
          case jo: JObject => Some(jo.values.toSeq)
          case jx =>
            throw new IllegalArgumentException(s"JSON primitive encountered: $jx")
        }
      case None => None
    }
  }

  override def open(): Unit = device.open()

  override def close(): Unit = device.close()

}
