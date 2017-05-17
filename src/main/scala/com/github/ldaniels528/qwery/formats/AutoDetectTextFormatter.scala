package com.github.ldaniels528.qwery.formats

import com.github.ldaniels528.qwery.formats.AutoDetectTextFormatter._
import com.github.ldaniels528.qwery.ops.{ResultSet, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Auto-Detecting Text Formatter
  * @author lawrence.daniels@gmail.com
  */
class AutoDetectTextFormatter extends TextFormatter {
  private var incoming: List[String] = Nil
  private var outgoing: List[Row] = Nil
  private var textFormatter: Option[TextFormatter] = None

  override def fromText(line: String): Row = {
    if (textFormatter.isEmpty) {
      incoming = line :: incoming
      if (incoming.size >= 5) {
        autodetectDelimiter(incoming) foreach { case (formatter, results) =>
          textFormatter = Option(formatter)
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
  * Auto-Detecting Text Formatter Companion
  * @author lawrence.daniels@gmail.com
  */
object AutoDetectTextFormatter {

  def autodetectDelimiter(sampleLines: List[String]): Option[(TextFormatter, ResultSet)] = {
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
    } yield (new CSVFormatter(delimiter = String.valueOf(delimiter), headers = headers), rows)
  }

}