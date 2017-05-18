package com.github.ldaniels528.qwery.formats

import com.github.ldaniels528.qwery.ops.Row
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.compactRender
import net.liftweb.json.{JObject, parse}
import org.slf4j.LoggerFactory

/**
  * JSON Format
  * @author lawrence.daniels@gmail.com
  */
case class JSONFormat() extends TextFormat {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val formats = net.liftweb.json.DefaultFormats

  override def fromText(line: String): Row = {
    parse(line) match {
      case jo: JObject => jo.values.toSeq
      case jx =>
        logger.warn(s"JSON primitive encountered: $jx")
        Nil
    }
  }

  override def toText(row: Row): Seq[String] = {
    val data = compactRender(decompose(Map(row: _*)))
    Seq(data)
  }

}
