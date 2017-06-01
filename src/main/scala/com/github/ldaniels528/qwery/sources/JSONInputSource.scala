package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, KafkaInputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row}
import net.liftweb.json.{JObject, parse}

/**
  * JSON Input Source
  * @author lawrence.daniels@gmail.com
  */
case class JSONInputSource(device: InputDevice, hints: Option[Hints] = None) extends InputSource {

  override def read(): Option[Row] = {
    device.read() match {
      case Some(Record(bytes, _, _)) =>
        parse(new String(bytes)) match {
          case jo: JObject => Some(jo.values.toSeq)
          case jx =>
            throw new IllegalArgumentException(s"JSON primitive encountered: $jx")
        }
      case None => None
    }
  }

}

/**
  * JSON Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object JSONInputSource extends InputSourceFactory {
  private val kafkaJsonHeader = "kafka:json://"

  def apply(device: InputDevice, schemaString: String, hints: Option[Hints]): JSONInputSource = {
    JSONInputSource(device, hints)
  }

  override def apply(path: String, hints: Option[Hints]): Option[JSONInputSource] = {
    Option(parseURI(path, hints))
  }

  /**
    * Returns a Kafka-JSON Input Source
    * @param uri the given URI (e.g. 'kafka:json://server?topic=X&group_id=Y')
    * @return an [[JSONInputSource]] instance
    */
  def parseURI(uri: String, hints: Option[Hints]): JSONInputSource = {
    uri.drop(kafkaJsonHeader.length).split("[?]") match {
      case Array(server, queryString) =>
        val params = Map(queryString.split("[&]") map { param =>
          param.split("[=]") match {
            case Array(key, value) => key -> value
            case _ =>
              throw new IllegalArgumentException(s"Invalid Kafka-JSON URL: $param")
          }
        }: _*)
        val topic = params.getOrElse("topic", throw new IllegalArgumentException(s"Invalid Kafka-JSON URL: topic is missing"))
        val groupId = params.getOrElse("group_id", throw new IllegalArgumentException(s"Invalid Kafka-JSON URL: group_id is missing"))
        JSONInputSource(KafkaInputDevice(
          topic = topic, groupId = groupId, bootstrapServers = server, hints.flatMap(_.properties)), hints = hints)
      case _ =>
        throw new IllegalArgumentException(s"Invalid Kafka-JSON URL: $uri")
    }
  }

  override def understands(url: String): Boolean = url.startsWith(kafkaJsonHeader)

}