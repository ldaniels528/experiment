package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.util.ResourceHelper._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

/**
  * Represents an Avro Transcoding capability
  * @author lawrence.daniels@gmail.com
  */
trait AvroTranscoding {

  /**
    * Transforms the given JSON string into an Avro encoded-message using the given Avro schema
    * @param json the given JSON string
    * @param schema the given [[Schema]]
    * @return a byte array representing an Avro-encoded message
    */
  def transcodeJsonToAvroBytes(json: String, schema: Schema, encoding: String = "UTF8"): Array[Byte] = {
    new ByteArrayInputStream(json.getBytes(encoding)) use { in =>
      new ByteArrayOutputStream(json.length) use { out =>
        // setup the reader, writer, encoder and decoder
        val reader = new GenericDatumReader[Object](schema)
        val writer = new GenericDatumWriter[Object](schema)

        // transform the JSON into Avro
        val decoder = DecoderFactory.get().jsonDecoder(schema, in)
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        val datum = reader.read(null, decoder)
        writer.write(datum, encoder)
        encoder.flush()
        out.toByteArray
      }
    }
  }

  /**
    * Transforms the given Avro-encoded binary message into an Avro-specific JSON string
    * @param record the given [[GenericRecord]]
    * @return an Avro-specific JSON string
    */
  def transcodeRecordToAvroJson(record: GenericRecord, encoding: String = "UTF8"): String = {
    transcodeAvroBytesToAvroJson(record.getSchema, encodeRecord(record), encoding)
  }

  /**
    * Transforms the given Avro-encoded binary message into an Avro-specific JSON string
    * @param schema the given [[Schema]]
    * @param bytes the given Avro-encoded binary message
    * @return an Avro-specific JSON string
    */
  def transcodeAvroBytesToAvroJson(schema: Schema, bytes: Array[Byte], encoding: String = "UTF8"): String = {
    new ByteArrayOutputStream(bytes.length * 4) use { out =>
      val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
      val encoder = EncoderFactory.get().jsonEncoder(schema, out)

      // transform the bytes into Avro-specific JSON
      val reader = new GenericDatumReader[Object](schema)
      val writer = new GenericDatumWriter[Object](schema)
      val datum = reader.read(null, decoder)
      writer.write(datum, encoder)
      encoder.flush()
      out.toString(encoding)
    }
  }

  /**
    * Converts the given byte array to an Avro Generic Record
    * @param schema the given Avro Schema
    * @param bytes the given byte array
    * @return an Avro Generic Record
    */
  def decodeRecord(schema: Schema, bytes: Array[Byte]): GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null, decoder)
  }

  /**
    * Converts an Avro Java Bean into a byte array
    * @param datum the given Avro Java Bean
    * @return a byte array
    */
  def encodeRecord[T <: GenericRecord](datum: T): Array[Byte] = {
    new ByteArrayOutputStream(1024) use { out =>
      val writer = new GenericDatumWriter[GenericRecord](datum.getSchema)
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(datum, encoder)
      encoder.flush()
      out.toByteArray
    }
  }

}
