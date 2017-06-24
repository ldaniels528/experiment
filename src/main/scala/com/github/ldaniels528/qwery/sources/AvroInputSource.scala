package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.util.JSONSupport
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Represents an Avro Input Source
  * @author lawrence.daniels@gmail.com
  */
case class AvroInputSource(device: InputDevice, hints: Option[Hints]) extends InputSource with JSONSupport {
  private lazy val schemaString = hints.flatMap(_.avro).getOrElse("No Avro schema was specified")
  private lazy val schema = new Schema.Parser().parse(schemaString)
  private lazy val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
  private lazy val log = LoggerFactory.getLogger(getClass)

  override def read(scope: Scope): Option[Row] = {
    device.read() map { case Record(bytes, offset, _) =>
      converter.invert(bytes).flatMap(message => Try(parseRow(message.toString))) match {
        case Success(row) => row
        case Failure(e) =>
          log.error(s"Avro decode of record # $offset failed: ${e.getMessage}", e)
          Row.empty
      }
    }
  }

}
