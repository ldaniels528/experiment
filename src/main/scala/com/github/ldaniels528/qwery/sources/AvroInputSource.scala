package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, KafkaInputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.parse
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success}

/**
  * Represents an Avro Input Source
  * @author lawrence.daniels@gmail.com
  */
case class AvroInputSource(device: InputDevice, schema: Schema, hints: Option[Hints]) extends InputSource {
  private lazy val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
  private lazy val log = LoggerFactory.getLogger(getClass)

  override def close(): Unit = device.close()

  override def open(scope: Scope): Unit = device.open(scope)

  override def read(): Option[Row] = {
    device.read() map { case Record(bytes, offset, _) =>
      converter.invert(bytes).map(message => parse(message.toString)) match {
        case Success(jo: JObject) => jo.values.toSeq
        case Success(jv) =>
          log.warn(s"JSON primitive value returned: $jv")
          Nil
        case Failure(e) =>
          log.error(s"Avro decode of record # $offset failed: ${e.getMessage}", e)
          Nil
      }
    }
  }

}

/**
  * Avro Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object AvroInputSource extends InputSourceFactory {
  private val header = "kafka:avro://"

  def apply(device: InputDevice, schemaString: String, hints: Option[Hints]): AvroInputSource = {
    AvroInputSource(device, schema = new Schema.Parser().parse(schemaString), hints)
  }

  override def apply(path: String, hints: Option[Hints]): Option[AvroInputSource] = {
    Option(parseURI(path, hints))
  }

  /**
    * Returns a Kafka-Avro Input Source
    * @param uri the given URI (e.g. 'kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json')
    * @return an [[AvroInputSource]] instance
    */
  def parseURI(uri: String, hints: Option[Hints]): AvroInputSource = {
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
        val groupId = params.getOrElse("group_id", throw new IllegalArgumentException(s"Invalid Kafka-Avro URL: group_id is missing"))
        val schemaPath = params.getOrElse("schema", throw new IllegalArgumentException(s"Invalid Kafka-Avro URL: schema is missing"))
        val schemaString = Source.fromFile(schemaPath).mkString
        AvroInputSource(KafkaInputDevice(
          topic = topic, groupId = groupId, bootstrapServers = server, hints.flatMap(_.properties).orNull),
          schemaString = schemaString, hints = hints)
      case _ =>
        throw new IllegalArgumentException(s"Invalid Kafka-Avro URL: $uri")
    }
  }

  override def understands(url: String): Boolean = url.startsWith(header)

}