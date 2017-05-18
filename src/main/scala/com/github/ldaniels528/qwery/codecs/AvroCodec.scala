package com.github.ldaniels528.qwery.codecs

import com.github.ldaniels528.qwery.ops.Row
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import net.liftweb.json.JsonAST.JObject
import net.liftweb.json.parse
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Represents an Avro CODEC
  * @author lawrence.daniels@gmail.com
  */
class AvroCodec(schema: Schema) extends Codec[Array[Byte]] {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  override def encode(row: Row): Array[Byte] = ???

  override def decode(bytes: Array[Byte]): Try[Row] = {
    converter.invert(bytes)
      .map(message => parse(message.toString))
      .map {
        case jo: JObject => jo.values.toSeq
        case jv => Nil
      }
  }

}
