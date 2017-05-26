package com.github.ldaniels528.qwery.devices

import java.util.concurrent.{Future => JFuture}
import java.util.{Properties => JProperties}

import com.github.ldaniels528.qwery.ops.Scope
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * Kafka Output Device
  * @author lawrence.daniels@gmail.com
  */
case class KafkaOutputDevice(topic: String, config: JProperties) extends OutputDevice {
  private var producer: Option[KafkaProducer[Array[Byte], Array[Byte]]] = None

  override def open(scope: Scope): Unit = {
    producer = Option(new KafkaProducer(config))
  }

  override def close(): Unit = {
    producer.foreach(_.close())
    producer = None
  }

  override def write(record: Record): Option[JFuture[RecordMetadata]] = {
    producer.map(_.send(new ProducerRecord(topic, Array[Byte](0), record.data)))
  }

}

/**
  * Kafka Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object KafkaOutputDevice {

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

} 