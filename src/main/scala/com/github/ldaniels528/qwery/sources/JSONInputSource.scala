package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import net.liftweb.json.{JObject, parse}

/**
  * JSON Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONInputSource(device: InputDevice, hints: Option[Hints] = None) extends InputSource {

  override def close(): Unit = device.close()

  override def open(scope: Scope): Unit = device.open(scope)

  override def read(): Option[Row] = {
    device.read() match {
      case Some(Record(bytes, _, _)) =>
        parse(new String(bytes)) match {
          case jo: JObject => Some(jo.values.toSeq)
          case jx =>
            throw new IllegalArgumentException(s"JSON primitive encountered: $jx")
        }
      case None => None
    }
  }

}
