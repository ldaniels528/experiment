package com.github.ldaniels528.qwery.formats

import com.github.ldaniels528.qwery.formats.AutoDetectTextFormat._
import com.github.ldaniels528.qwery.ops.{ResultSet, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Auto-Detecting Text Format
  * @author lawrence.daniels@gmail.com
  */
class AutoDetectTextFormat extends TextFormat {
  private var incoming: List[String] = Nil
  private var outgoing: List[Row] = Nil
  private var textFormat: Option[TextFormat] = None

  override def fromText(line: String): Row = {
    if (textFormat.isEmpty) {
      incoming = line :: incoming
      if (incoming.size >= 5) {
        autodetectDelimiter(incoming) foreach { case (formatter, results) =>
          textFormat = Option(formatter)
          outgoing = results.toList
          incoming = Nil
        }
      }
    }
    Nil // TODO need to move to an asynchronous interface
  }

  override def toText(row: Row): Seq[String] = {
    throw new IllegalArgumentException("Encoding is not supported")
  }

}

/**
  * Auto-Detecting Text Format Companion
  * @author lawrence.daniels@gmail.com
  */
object AutoDetectTextFormat {

  def autodetectDelimiter(sampleLines: List[String]): Option[(TextFormat, ResultSet)] = {
    // identify the potential delimiters (from the header line)
    val delimiters = sampleLines.headOption map { header =>
      header.collect {
        case c if !c.isLetterOrDigit & c != '"' => c
      }.distinct
    } map (_.toCharArray.toList) getOrElse Nil

    // find a delimiter where splitting all lines results in the same number of elements
    val delimiter_? = delimiters.find { delimiter =>
      sampleLines.headOption.map(_.delimitedSplit(delimiter).length).exists { length =>
        sampleLines.forall(_.delimitedSplit(delimiter).length == length)
      }
    }

    for {
      delimiter <- delimiter_?
      headers <- sampleLines.headOption.map(_.delimitedSplit(delimiter))
      rows = sampleLines.tail.map(line => headers zip line.delimitedSplit(delimiter))
    } yield (new CSVFormat(delimiter = String.valueOf(delimiter), headers = headers), rows)
  }

}