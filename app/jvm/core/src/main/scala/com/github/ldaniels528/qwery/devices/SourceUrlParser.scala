package com.github.ldaniels528.qwery.devices

import com.github.ldaniels528.qwery.devices.SourceUrlParser.URLComps
import com.github.ldaniels528.qwery.ops.Hints
import com.github.ldaniels528.qwery.sources._
import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Source URL Parser
  * @author lawrence.daniels@gmail.com
  */
trait SourceUrlParser {

  /**
    * Parses the path or URL and returns an input source
    * @param path the given URL (e.g. "kafka://server?topic=X&group_id=Y&schema=/path/to/schema.json")
    * @return the option of an [[InputSource input source]]
    */
  def parseInputSource(path: String, hints: Option[Hints]): Option[InputSource] = {
    for {
      device <- InputDeviceFactory.parseInputURL(path, hints)
      source <- findInputSource(device, path, hints) ?? guessInputSourceFormat(device, path, hints)
    } yield source
  }

  /**
    * Parses the path or URL and returns an input source
    * @param path the given URL (e.g. "kafka://server?topic=X&group_id=Y&schema=/path/to/schema.json")
    * @return the option of an [[OutputSource output source]]
    */
  def parseOutputSource(path: String, hints: Option[Hints]): Option[OutputSource] = {
    for {
      device <- OutputDeviceFactory.parseOutputURL(path, hints)
      source <- findOutputSource(device, path, hints) ?? guessOutputSourceFormat(device, path, hints)
    } yield source
  }

  private def findInputSource(device: InputDevice, path: String, hints: Option[Hints]): Option[InputSource] = {
    hints flatMap {
      case h if h.avro.nonEmpty => Option(AvroInputSource(device, hints))
      case h if h.delimiter.nonEmpty => Option(DelimitedInputSource(device, hints))
      case h if h.fixed.contains(true) => Option(FixedWidthInputSource(device, hints))
      case h if h.isJson.contains(true) | h.jsonPath.nonEmpty => Option(JSONInputSource(device, hints))
      case _ =>
        device match {
          case source: InputSource => Option(source)
          case _ => None
        }
    }
  }

  private def findOutputSource(device: OutputDevice, path: String, hints: Option[Hints]): Option[OutputSource] = {
    hints flatMap {
      case h if h.avro.nonEmpty => Option(AvroOutputSource(device, hints))
      case h if h.delimiter.nonEmpty => Option(DelimitedOutputSource(device, hints))
      case h if h.fixed.contains(true) => Option(FixedWidthOutputSource(device, hints))
      case h if h.isJson.contains(true) | h.jsonPath.nonEmpty => Option(JSONOutputSource(device, hints))
      case _ =>
        device match {
          case source: OutputSource => Option(source)
          case _ => None
        }
    }
  }

  private def guessInputSourceFormat(device: InputDevice, path: String, hints: Option[Hints]) = {
    def guessFormat(mockFile: String, hints: Option[Hints]): Option[InputSource] = mockFile match {
      case file if file.endsWith(".gz") => guessFormat(file.drop(3), hints = hints.map(_.copy(gzip = Some(true))))
      case file if file.endsWith(".csv") => Option(DelimitedInputSource(device, hints = hints.map(_.asCSV)))
      case file if file.endsWith(".json") => Option(JSONInputSource(device, hints = hints.map(_.asJSON)))
      case file if file.endsWith(".psv") => Option(DelimitedInputSource(device, hints = hints.map(_.asPSV)))
      case file if file.endsWith(".tsv") => Option(DelimitedInputSource(device, hints = hints.map(_.asTSV)))
      case _ => None
    }

    guessFormat(path.toLowerCase(), hints = hints ?? Some(Hints()))
  }

  private def guessOutputSourceFormat(device: OutputDevice, path: String, hints: Option[Hints]) = {
    def guessFormat(mockFile: String, hints: Option[Hints]): Option[OutputSource] = mockFile match {
      case file if file.endsWith(".gz") => guessFormat(file.drop(3), hints = hints.map(_.copy(gzip = Some(true))))
      case file if file.endsWith(".csv") => Option(DelimitedOutputSource(device, hints = hints.map(_.asCSV)))
      case file if file.endsWith(".json") => Option(JSONOutputSource(device, hints = hints.map(_.asJSON)))
      case file if file.endsWith(".psv") => Option(DelimitedOutputSource(device, hints = hints.map(_.asPSV)))
      case file if file.endsWith(".tsv") => Option(DelimitedOutputSource(device, hints = hints.map(_.asTSV)))
      case _ => None
    }

    guessFormat(path.toLowerCase(), hints = hints ?? Some(Hints()))
  }

  protected def parseURI(uri: String): URLComps = {
    val uriRegExA = "^(\\S+)://(\\S+)[?](.*)".r
    val uriRegExB = "^(\\S+)://(\\S+)".r

    val (prefix, host, path, queryString) = uri match {
      case uriRegExA(_prefix, hostAndPath, _queryString) =>
        val (_host, _path) = parseHost(hostAndPath)
        (_prefix, _host, _path, _queryString)
      case uriRegExB(_prefix, hostAndPath) =>
        val (_host, _path) = parseHost(hostAndPath)
        (_prefix, _host, _path, "")
      case _path => ("", None, Some(_path), "")
    }

    val params = Map(queryString.split("[&]").flatMap(_.split("[=]") match {
      case Array(key, value) => Some(key -> value)
      case _ => None
    }): _*)

    URLComps(prefix = prefix, host = host, path = path, params = params)
  }

  private def parseHost(hostAndPathString: String): (Option[String], Option[String]) = {
    // is a host being referenced?
    hostAndPathString.lastIndexOf('/') match {
      case -1 => (Some(hostAndPathString), None)
      case index =>
        (Some(hostAndPathString.substring(0, index)), Some(hostAndPathString.substring(index + 1)))
    }
  }

}

/**
  * Source URL Parser Companion
  * @author lawrence.daniels@gmail.com
  */
object SourceUrlParser {

  case class URLComps(prefix: String, host: Option[String], path: Option[String], params: Map[String, String]) {
    override def toString = s"${getClass.getSimpleName}(prefix = '$prefix', host = '${host.orNull}', path = '${path.orNull}', params = $params)"
  }

}