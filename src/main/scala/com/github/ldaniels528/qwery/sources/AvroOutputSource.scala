package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.{Hints, Row}
import com.github.ldaniels528.qwery.util.JSONSupport
import org.apache.avro.Schema

/**
  * Represents an Avro Output Source
  * @author lawrence.daniels@gmail.com
  */
case class AvroOutputSource(device: OutputDevice, hints: Option[Hints])
  extends OutputSource with AvroTranscoding with JSONSupport {
  private lazy val schemaString = hints.flatMap(_.avro).getOrElse("No Avro schema was specified")
  private lazy val schema = new Schema.Parser().parse(schemaString)
  private var offset = 0L

  override def write(row: Row): Unit = {
    offset += 1
    val bytes = transcodeJsonToAvroBytes(json = toJson(row), schema = schema)
    device.write(Record(bytes, offset))
  }

}

