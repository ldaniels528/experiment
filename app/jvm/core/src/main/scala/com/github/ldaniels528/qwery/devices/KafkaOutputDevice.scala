package com.github.ldaniels528.qwery.devices

import java.util.concurrent.{Future => JFuture}
import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.ops.{Hints, Scope}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Kafka Output Device
  * @author lawrence.daniels@gmail.com
  */
case class KafkaOutputDevice(topic: String, config: JProperties) extends OutputDevice {
  private var producer: Option[KafkaProducer[Array[Byte], Array[Byte]]] = None

  override def close(): Unit = {
    producer.foreach(_.close())
    producer = None
  }

  override def open(scope: Scope): Unit = {
    super.open(scope)
    producer = Option(new KafkaProducer(config))
  }

  override def write(record: Record): Option[JFuture[RecordMetadata]] = {
    statsGen.update(records = 1, bytesRead = record.data.length)
    producer.map(_.send(new ProducerRecord(topic, Array[Byte](0), record.data)))
  }

}

/**
  * Kafka Output Device Companion
  * @author lawrence.daniels@gmail.com
  */
object KafkaOutputDevice extends OutputDeviceFactory with SourceUrlParser {

  def apply(topic: String, bootstrapServers: String, producerProps: Option[JProperties] = None): KafkaOutputDevice = {
    KafkaOutputDevice(topic, {
      val props = new JProperties()
      props.put("bootstrap.servers", bootstrapServers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      producerProps foreach props.putAll
      props
    })
  }

  /**
    * Returns a compatible output device for the given URL.
    * @param url the given URL (e.g. "kafka://server?topic=X&group_id=Y")
    * @return an option of the [[OutputDevice output device]]
    */
  override def parseOutputURL(url: String, hints: Option[Hints]): Option[OutputDevice] = {
    val comps = parseURI(url)
    for {
      bootstrapServers <- comps.host if url.toLowerCase.startsWith("kafka:")
      topic <- comps.params.get("topic")
      groupId <- comps.params.get("group_id")
      config = hints.flatMap(_.properties)
    } yield KafkaOutputDevice(topic = topic, bootstrapServers = bootstrapServers, producerProps = config)
  }

}