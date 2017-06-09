package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row, Scope}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.parse
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * Represents an Avro Input Source
  * @author lawrence.daniels@gmail.com
  */
case class AvroInputSource(device: InputDevice, hints: Option[Hints]) extends InputSource {
  private lazy val schemaString = hints.flatMap(_.avro).getOrElse("No Avro schema was specified")
  private lazy val schema = new Schema.Parser().parse(schemaString)
  private lazy val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
  private lazy val log = LoggerFactory.getLogger(getClass)

  override def read(scope: Scope): Option[Row] = {
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
  * Avro Input Source Singleton
  * @author lawrence.daniels@gmail.com
  */
object AvroInputSource extends InputSourceFactory {

  override def findInputSource(device: InputDevice, hints: Option[Hints]): Option[InputSource] = {
    if (hints.exists(_.avro.nonEmpty)) Option(AvroInputSource(device, hints)) else None
  }

}