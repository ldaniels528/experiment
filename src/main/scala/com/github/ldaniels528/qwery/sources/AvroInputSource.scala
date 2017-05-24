package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Row, Scope}
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
case class AvroInputSource(device: InputDevice, schema: Schema) extends InputSource {
  private lazy val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
  private lazy val log = LoggerFactory.getLogger(getClass)

  override def open(scope: Scope): Unit = device.open(scope)

  override def close(): Unit = device.close()

  override def read(): Option[Row] = {
    device.read() map { case Record(offset, bytes) =>
      converter.invert(bytes)
        .map(message => parse(message.toString)) match {
        case Success(jo: JObject) => jo.values.toSeq
        case Success(jv) => Nil
        case Failure(e) =>
          log.error(s"Record offset $offset failed", e)
          Nil
      }
    }
  }

}
