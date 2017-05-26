package com.github.ldaniels528.qwery.devices

import java.util.{Properties => JProperties}

import akka.actor.ActorRef
import com.github.ldaniels528.qwery.ops.Scope
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.language.postfixOps

/**
  * Kafka Input Device
  * @author lawrence.daniels@gmail.com
  */
case class KafkaInputDevice(topic: String, consumerProps: JProperties)
  extends InputDevice with AsyncInputDevice with RandomAccessInputDevice {
  private var consumer: Option[KafkaConsumer[String, Array[Byte]]] = None
  private var buffer: List[Record] = Nil

  override def close(): Unit = {
    consumer.foreach(_.close())
    consumer = None
  }

  override def fastForward(partitions: Seq[Int]): Unit = {
    consumer.foreach(_.seekToEnd(partitions.map(new TopicPartition(topic, _)).asJava))
  }

  override def open(scope: Scope): Unit = {
    consumer = Option(new KafkaConsumer[String, Array[Byte]](consumerProps))
    consumer.foreach(_.subscribe(List(topic).asJava))
  }

  override def read(actor: ActorRef) {
    consumer.foreach(_.poll(1).asScala foreach { rec =>
      actor ! Record(rec.value(), rec.offset())
    })
  }

  override def read(): Option[Record] = {
    if (buffer.size < 100) {
      consumer.foreach(_.poll(5000).asScala
        .foreach(rec => buffer = buffer ::: Record(rec.value, rec.offset, rec.partition) :: Nil))
    }

    // read the next row
    buffer match {
      case Nil => None
      case row :: remaining => buffer = remaining; Option(row)
    }
  }

  override def rewind(partitions: Seq[Int]): Unit = {
    consumer.foreach(_.seekToBeginning(partitions.map(new TopicPartition(topic, _)).asJava))
  }

  override def seek(partition: Int, offset: Long): Unit = {
    consumer.foreach(_.seek(new TopicPartition(topic, partition), offset))
  }

}

/**
  * Kafka Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object KafkaInputDevice {

  def apply(topic: String,
            groupId: String,
            bootstrapServers: String,
            consumerProps: JProperties = null): KafkaInputDevice = {
    KafkaInputDevice(topic, {
      val props = new JProperties()
      props.put("group.id", groupId)
      props.put("bootstrap.servers", bootstrapServers)
      props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      if (consumerProps != null) props.putAll(consumerProps)
      props
    })
  }
}