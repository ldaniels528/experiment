package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.{Hints, Row}
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.compactRender
import org.apache.avro.Schema

/**
  * Represents an Avro Output Source
  * @author lawrence.daniels@gmail.com
  */
case class AvroOutputSource(device: OutputDevice, hints: Option[Hints])
  extends OutputSource with AvroTranscoding {
  private lazy val schemaString = hints.flatMap(_.avro).getOrElse("No Avro schema was specified")
  private lazy val schema = new Schema.Parser().parse(schemaString)
  private implicit val formats = net.liftweb.json.DefaultFormats
  private var offset = 0L

  override def write(row: Row): Unit = {
    offset += 1
    val bytes = transcodeJsonToAvroBytes(json = compactRender(decompose(Map(row: _*))), schema = schema)
    device.write(Record(bytes, offset))
  }

}

