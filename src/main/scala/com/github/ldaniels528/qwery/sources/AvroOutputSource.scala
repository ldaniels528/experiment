package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.compactRender
import org.apache.avro.Schema

import scala.io.Source

/**
  * Represents an Avro Output Source
  * @author lawrence.daniels@gmail.com
  */
case class AvroOutputSource(device: OutputDevice, schema: Schema, hints: Option[Hints])
  extends OutputSource with AvroTranscoding {
  private implicit val formats = net.liftweb.json.DefaultFormats
  private var offset = 0L

  override def close(): Unit = device.close()

  override def open(scope: Scope): Unit = device.open(scope)

  override def write(row: Row): Unit = {
    offset += 1
    val bytes = transcodeJsonToAvroBytes(json = compactRender(decompose(Map(row: _*))), schema = schema)
    device.write(Record(bytes, offset))
  }

}

/**
  * Avro Output Source Companion
  * @author lawrence.daniels@gmail.com
  */
object AvroOutputSource extends OutputSourceFactory {
  private val header = "kafka:avro://"

  def apply(device: OutputDevice, schemaString: String, hints: Option[Hints]): AvroOutputSource = {
    AvroOutputSource(device, schema = new Schema.Parser().parse(schemaString), hints)
  }

  override def apply(path: String, append: Boolean, hints: Option[Hints]): Option[AvroOutputSource] = {
    Option(parseURI(path, hints))
  }

  /**
    * Returns a Kafka-Avro Input Source
    * @param uri the given URI (e.g. 'kafka:avro://server?topic=X&schema=/path/to/schema.json')
    * @return an [[AvroInputSource]] instance
    */
  def parseURI(uri: String, hints: Option[Hints]): AvroOutputSource = {
    uri.drop(header.length).split("[?]") match {
      case Array(server, queryString) =>
        val params = Map(queryString.split("[&]") map { param =>
          param.split("[=]") match {
            case Array(key, value) => key -> value
            case _ =>
              throw new IllegalArgumentException(s"Invalid Kafka-Avro URL: $param")
          }
        }: _*)
        val topic = params.getOrElse("topic", throw new IllegalArgumentException(s"Invalid Kafka-Avro URL: topic is missing"))
        val schemaPath = params.getOrElse("schema", throw new IllegalArgumentException(s"Invalid Kafka-Avro URL: schema is missing"))
        val schemaString = Source.fromFile(schemaPath).mkString
        AvroOutputSource(KafkaOutputDevice(
          topic = topic, bootstrapServers = server, hints.flatMap(_.properties).orNull),
          schemaString = schemaString, hints = hints)
      case _ =>
        throw new IllegalArgumentException(s"Invalid Kafka-Avro URL: $uri")
    }
  }

  override def understands(url: String): Boolean = url.startsWith(header)

}
