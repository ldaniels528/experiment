package com.github.ldaniels528.qwery.devices

import java.util.{Properties => JProperties}

import akka.actor.ActorRef
import com.github.ldaniels528.qwery.ops.Scope
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.language.postfixOps

/**
  * Kafka Topic Input Device
  * @author lawrence.daniels@gmail.com
  */
case class KafkaHLConsumerInputDevice(topic: String, consumerProps: JProperties)
  extends InputDevice with AsyncInputDevice with RandomAccessInputDevice {
  private var consumer: KafkaConsumer[String, Array[Byte]] = _
  private var buffer: List[Record] = Nil
  var connected = false

  override def close(): Unit = {
    connected = false
    consumer.close()
  }

  override def fastForward(partitions: Seq[Int]): Unit = {
    consumer.seekToEnd(partitions.map(new TopicPartition(topic, _)).asJava)
  }

  override def open(scope: Scope): Unit = {
    consumer = new KafkaConsumer[String, Array[Byte]](consumerProps)
    consumer.subscribe(List(topic).asJava)
    connected = true
  }

  override def read(actor: ActorRef) {
    consumer.poll(1).asScala foreach { rec =>
      actor ! Record(rec.offset(), rec.value())
    }
  }

  override def read(): Option[Record] = {
    if (buffer.isEmpty) {
      consumer.poll(5000).asScala
        .foreach(rec => buffer = buffer ::: Record(rec.offset, rec.value) :: Nil)
    }

    // read the next row
    buffer match {
      case Nil => None
      case row :: remaining => buffer = remaining; Option(row)
    }
  }

  override def rewind(partitions: Seq[Int]): Unit = {
    consumer.seekToBeginning(partitions.map(new TopicPartition(topic, _)).asJava)
  }

  override def seek(partition: Int, offset: Long): Unit = {
    consumer.seek(new TopicPartition(topic, partition), offset)
  }

}

/**
  * Kafka Topic Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object KafkaHLConsumerInputDevice {

  def apply(topic: String,
            groupId: String,
            bootstrapServers: String,
            consumerProps: JProperties = null): KafkaHLConsumerInputDevice = {
    KafkaHLConsumerInputDevice(topic, {
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