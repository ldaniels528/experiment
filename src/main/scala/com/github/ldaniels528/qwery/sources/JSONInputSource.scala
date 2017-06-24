package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import com.github.ldaniels528.qwery.util.JSONSupport
import play.api.libs.json._

import scala.language.postfixOps

/**
  * JSON Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONInputSource(device: InputDevice, hints: Option[Hints]) extends InputSource with JSONSupport {
  private var buffer: List[Row] = Nil
  private val jsonPath = hints.map(_.jsonPath).getOrElse(Nil)

  override def read(scope: Scope): Option[Row] = {
    if (buffer.nonEmpty) getNext
    else {
      device.read() match {
        case Some(Record(bytes, _, _)) =>
          val json = jsonPath.map(_.getAsString(scope)).foldLeft[JsValue](Json.parse(new String(bytes))) { (jv, elem) =>
            elem.map(text => (jv \ text).get) getOrElse jv
          }
          val rows = parseRows(json)
          buffer = rows ::: buffer
          getNext
        case None => getNext
      }
    }
  }

  private def getNext: Option[Row] = {
    if (buffer.isEmpty) None else {
      val row = buffer.headOption
      buffer = buffer.tail
      row
    }
  }

}
