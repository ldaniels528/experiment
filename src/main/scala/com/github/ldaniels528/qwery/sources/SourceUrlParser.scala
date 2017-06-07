package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.util.OptionHelper._
import com.github.ldaniels528.qwery.devices._
import com.github.ldaniels528.qwery.ops.Hints
import com.github.ldaniels528.qwery.sources.SourceUrlParser._

import scala.io.Source

/**
  * Source URL Parser
  * @author lawrence.daniels@gmail.com
  */
trait SourceUrlParser {

  /**
    * Parses the path or URL and returns an input source
    * @param path the given URL (e.g. "kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json")
    * @return the option of an [[InputSource input source]]
    */
  def parseInputSource(path: String, hints: Option[Hints]): Option[InputSource] = {
    val myHints = hints ?? Some(Hints())
    val regex = "^(\\S+):(\\S+):(.*)".r
    path match {
      // http://www.nasdaq.com/symbol/abe
      case uri if uri.startsWith("http://") | uri.startsWith("https://") =>
        guessInputDevice(path, hints).map(DelimitedInputSource(_, myHints))
      // kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json
      case regex(deviceType, format, uri) =>
        val comps = parseURI(uri)
        val inputDevice = findInputDevice(deviceType, comps, hints)
        inputDevice.flatMap(findInputSource(format, _, comps, hints))
      // ./companylist.csv
      case file if file.endsWith(".csv") => Option(DelimitedInputSource(TextFileInputDevice(path), hints = myHints.map(_.asCSV)))
      case file if file.endsWith(".json") => Option(JSONInputSource(TextFileInputDevice(path), hints = myHints.map(_.asJSON)))
      case file if file.endsWith(".psv") => Option(DelimitedInputSource(TextFileInputDevice(path), hints = myHints.map(_.asPSV)))
      case file if file.endsWith(".tsv") => Option(DelimitedInputSource(TextFileInputDevice(path), hints = myHints.map(_.asTSV)))
      case _ => None
    }
  }

  def findInputDevice(deviceType: String, comps: URLComps, hints: Option[Hints]): Option[InputDevice] = {
    deviceType match {
      // kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json
      case s if s.equalsIgnoreCase("kafka") =>
        for {
          bootstrapServers <- comps.host
          topic <- comps.params.get("topic")
          groupId <- comps.params.get("group_id")
          config = hints.flatMap(_.properties)
        } yield KafkaInputDevice(bootstrapServers = bootstrapServers, topic = topic, groupId = groupId, consumerProps = config)
      // s3:csv://ldaniels3/companylist.csv?region=us-west-1
      case s if s.equalsIgnoreCase("s3") =>
        for {
          bucket <- comps.host
          key <- comps.path
          config <- hints.flatMap(_.properties)
        } yield AWSS3InputDevice(bucketName = bucket, keyName = key, config = config)
      // undetermined
      case _ => None
    }
  }

  def guessInputDevice(path: String, hints: Option[Hints]): Option[InputDevice] = {
    val myHints = hints ?? Some(Hints())
    path match {
      case file if file.endsWith(".csv") => Option(TextFileInputDevice(path))
      case file if file.endsWith(".json") => Option(TextFileInputDevice(path))
      case file if file.endsWith(".psv") => Option(TextFileInputDevice(path))
      case file if file.endsWith(".tsv") => Option(TextFileInputDevice(path))
      case _ => None
    }
  }

  def findInputSource(format: String, device: InputDevice, comps: URLComps, hints: Option[Hints]): Option[InputSource] = {
    val myHints = hints ?? Some(Hints())
    format match {
      // kafka:avro://server?topic=X&group_id=Y&schema=/path/to/schema.json
      case s if s.equalsIgnoreCase("avro") =>
        val schemaPath = comps.params.getOrElse("schema",
          throw new IllegalArgumentException("Avro Schema path is missing: Use 'schema=...' in URL."))
        val schemaString = Source.fromFile(schemaPath).mkString
        Option(AvroInputSource(device, schemaString, myHints.map(_.asCSV)))
      case s if s.equalsIgnoreCase("csv") => Option(DelimitedInputSource(device, myHints.map(_.asCSV)))
      case s if s.equalsIgnoreCase("json") => Option(JSONInputSource(device, myHints.map(_.asJSON)))
      case s if s.equalsIgnoreCase("psv") => Option(DelimitedInputSource(device, myHints.map(_.asPSV)))
      case s if s.equalsIgnoreCase("tsv") => Option(DelimitedInputSource(device, myHints.map(_.asTSV)))
      case _ => None
    }
  }

  private def parseURI(uri: String): URLComps = {
    // are there parameters?
    uri.indexOf('?') match {
      case -1 =>
        val (host, path) = parseHost(uri)
        URLComps(host = host, path = path, params = Map.empty)
      case index =>
        val (host, path) = parseHost(uri.substring(0, index))
        val paramString = uri.substring(index + 1)
        val params = Map(paramString.split("[&]").flatMap(_.split("[=]") match {
          case Array(key, value) => Some(key -> value)
          case _ => None
        }): _*)
        URLComps(host = host, path = path, params = params)
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
  * Source URL Parser Companion
  * @author lawrence.daniels@gmail.com
  */
object SourceUrlParser {

  case class URLComps(host: Option[String], path: Option[String], params: Map[String, String])

}