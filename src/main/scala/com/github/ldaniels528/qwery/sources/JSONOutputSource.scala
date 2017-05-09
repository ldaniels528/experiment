package com.github.ldaniels528.qwery.sources

import java.io.{BufferedWriter, File, FileWriter}

import net.liftweb.json.Extraction._
import net.liftweb.json.JsonAST._

/**
  * JSON Output Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONOutputSource(file: File) extends QueryOutputSource {
  private lazy val writer = new BufferedWriter(new FileWriter(file))
  implicit val formats = net.liftweb.json.DefaultFormats

  override def close(): Unit = writer.close()

  override def flush(): Unit = writer.flush()

  override def write(data: Seq[(String, Any)]): Unit = {
    val line = compactRender(decompose(Map(data: _*)))
    writer.write(line)
    writer.newLine()
  }

}

/**
  * JSON Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
object JSONOutputSource extends QueryOutputSourceFactory {

  override def apply(uri: String): Option[QueryOutputSource] = {
    if (understands(uri))
      Option(JSONOutputSource(new File(uri)))
    else
      None
  }

  override def understands(url: String): Boolean = url.toLowerCase().endsWith(".json")

}