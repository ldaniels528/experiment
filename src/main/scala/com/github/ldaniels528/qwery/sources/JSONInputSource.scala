package com.github.ldaniels528.qwery.sources

import java.io.File
import java.net.URL

import com.github.ldaniels528.qwery.ResultSet
import com.github.ldaniels528.qwery.ops.Scope
import net.liftweb.json._

import scala.io.Source

/**
  * JSON Input Source
  * @author lawrence.daniels@gmail.com
  */
class JSONInputSource(source: Source) extends QueryInputSource {

  override def execute(scope: Scope): ResultSet = {
    source.getLines().filter(_.trim.nonEmpty) map { line =>
      parse(line) match {
        case jo: JObject => jo.values.toSeq
        case jx =>
          println(s"jx => $jx")
          Nil
      }
    }
  }

}

/**
  * JSON Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
object JSONInputSource extends QueryInputSourceFactory {

  def apply(file: File): JSONFileInputSource = JSONFileInputSource(file)

  def apply(url: URL): JSONURLInputSource = JSONURLInputSource(url)

  override def apply(uri: String): Option[JSONInputSource] = {
    if (understands(uri))
      Option(JSONInputSource(new File(uri)))
    else
      None
  }

  override def understands(url: String): Boolean = url.toLowerCase().endsWith(".json")

}

case class JSONFileInputSource(file: File) extends JSONInputSource(Source.fromFile(file))

case class JSONURLInputSource(url: URL) extends JSONInputSource(Source.fromURL(url))