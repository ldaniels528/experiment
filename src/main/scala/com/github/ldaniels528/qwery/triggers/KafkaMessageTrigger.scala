package com.github.ldaniels528.qwery.triggers

import java.util.Properties

import com.github.ldaniels528.qwery.sources.{OutputDevice, Record}
import com.github.ldaniels528.qwery.triggers.KafkaMessageTrigger.SSLOptions
import com.github.ldaniels528.qwery.util.DurationHelper._
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

// TODO Consider making a distinction between streaming and triggered events

/**
  * Kafka Message Trigger
  * @author lawrence.daniels@gmail.com
  */
class KafkaMessageTrigger(bootstrapServers: String,
                          topic: String,
                          groupId: String,
                          decoder: OutputDevice,
                          sslOptions: Option[SSLOptions] = None) extends Trigger {
  private var alive: Boolean = _

  override def start()(implicit ec: ExecutionContext): Unit = Future {
    // create the consumer for our topic, and subscribe to the topic
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](getProperties(bootstrapServers, groupId, sslOptions))
    consumer.subscribe(List(topic).asJava)

    alive = true
    while (alive) {
      val records = consumer.poll(1.seconds).asScala
      for (record <- records) {
        decoder.write(Record(record.offset, record.value))
      }
    }
  }

  override def stop(): Unit = alive = false

  /**
    * Returns the Kafka consumer properties
    * @param groupId the given consumer group ID (e.g. "ldtest")
    * @return the Kafka consumer [[Properties properties]]
    */
  private def getProperties(bootstrapServers: String, groupId: String, sslOptions: Option[SSLOptions]) = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "latest")
    props.put("enable.auto.commit", false: java.lang.Boolean)
    sslOptions.foreach { options =>
      options.protocol.foreach(props.put("security.protocol", _))
      options.keyStoreLocation.foreach(props.put("ssl.keystore.location", _))
      options.keyStorePassword.foreach(props.put("ssl.keystore.password", _))
      options.trustStoreLocation.foreach(props.put("ssl.truststore.location", _))
      options.trustStorePassword.foreach(props.put("ssl.truststore.password", _))
      options.sslKeyPassword.foreach(props.put("ssl.key.password", _))
    }
    props
  }

}

/**
  * Kafka Message Trigger Companion
  * @author lawrence.daniels@gmail.com
  */
object KafkaMessageTrigger {

  case class SSLOptions(protocol: Option[String] = None, // SSL
                        sslKeyPassword: Option[String] = None,
                        keyStoreLocation: Option[String] = None,
                        keyStorePassword: Option[String] = None,
                        trustStoreLocation: Option[String] = None,
                        trustStorePassword: Option[String] = None)

}