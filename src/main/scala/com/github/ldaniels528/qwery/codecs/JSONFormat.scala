package com.github.ldaniels528.qwery.codecs

import com.github.ldaniels528.qwery.ops.Row
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.compactRender
import net.liftweb.json.{JObject, parse}

import scala.util.Try

/**
  * JSON Format
  * @author lawrence.daniels@gmail.com
  */
case class JSONFormat() extends TextFormat {
  private implicit val formats = net.liftweb.json.DefaultFormats

  override def decode(line: String): Try[Row] = Try {
    parse(line) match {
      case jo: JObject => jo.values.toSeq
      case jx =>
        throw new IllegalArgumentException(s"JSON primitive encountered: $jx")
    }
  }

  override def encode(row: Row): String = compactRender(decompose(Map(row: _*)))

}
