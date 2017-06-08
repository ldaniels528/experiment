package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row}
import net.liftweb.json.{JObject, parse}

/**
  * JSON Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONInputSource(device: InputDevice, hints: Option[Hints] = None) extends InputSource {

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

/**
  * JSON Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object JSONInputSource extends InputSourceFactory {

  override def findInputSource(device: InputDevice, hints: Option[Hints]): Option[InputSource] = {
    if (hints.exists(_.isJson.contains(true))) Option(JSONInputSource(device, hints)) else None
  }

}