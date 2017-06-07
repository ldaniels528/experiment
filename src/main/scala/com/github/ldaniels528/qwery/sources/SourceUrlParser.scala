package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.Hints
import com.github.ldaniels528.qwery.sources.SourceUrlParser.Comps

/**
  * Source URL Parser
  * @author lawrence.daniels@gmail.com
  */
trait SourceUrlParser {

  /**
    * Parses the URL and returns an input source
    * @param url the given URL (e.g. "kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json")
    * @return the option of an [[InputSource input source]]
    */
  def parseInputSource(url: String, hints: Option[Hints]): Option[InputSource] = {
    url.split("[:]", 3).toList match {
      case device :: format :: uri :: Nil =>
        println(s"format => $format, device => $device, uri => ${parseURI(uri)}")
        device match {
          // kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json
          case d if d.equalsIgnoreCase("kafka") =>
            hints.flatMap(_.properties) map { config =>
              KafkaInputDevice(topic = "", config = config)
            }
          // s3:csv://ldaniels3/companylist.csv?region=us-west-1
          case d if d.equalsIgnoreCase("s3") =>
            hints.flatMap(_.properties) map { config =>
              AWSS3InputDevice(bucketName = "ldaniels3", keyName = "companylist.csv", config = config)
            }
          case _ => None
        }
        None
      case _ => None
    }
  }

  private def parseURI(uri: String): Comps = {
    // are there parameters?
    uri.indexOf('?') match {
      case -1 =>
        val (host, path) = parseHost(uri)
        Comps(host = host, path = path, params = Nil)
      case index =>
        val (host, path) = parseHost(uri.substring(0, index))
        val paramString = uri.substring(index + 1)
        Comps(host = host, path = path, params = paramString.split("[&]"))
    }
  }

  private def parseHost(hostAndPathString: String): (Option[String], Option[String]) = {
    // is a host being referenced?
    if (hostAndPathString.startsWith("//")) {
      val hostString = hostAndPathString.drop(2)
      hostString.lastIndexOf('/') match {
        case -1 => (Some(hostString), None)
        case index =>
          (Some(hostString.substring(0, index)), Some(hostString.substring(index + 1)))
      }
    }
    else (None, Some(hostAndPathString))
  }

}

/**
  * Source URL Parser
  * @author lawrence.daniels@gmail.com
  */
object SourceUrlParser extends SourceUrlParser {

  case class Comps(host: Option[String], path: Option[String], params: Seq[String])

  def main(args: Array[String]): Unit = {
    parseInputSource("kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json", hints = None)
    parseInputSource("s3:csv://ldaniels3/companylist.csv?region=us-west-1", hints = None)
  }

}