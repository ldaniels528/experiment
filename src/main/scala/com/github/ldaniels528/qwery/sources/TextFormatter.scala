package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{ResultSet, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Text Formatter
  * @author lawrence.daniels@gmail.com
  */
trait TextFormatter {

  def fromText(line: String): Row

  def toText(row: Row): Seq[String]

}

/**
  * Text Formatter Singleton
  * @author lawrence.daniels@gmail.com
  */
object TextFormatter {

  def autodetectDelimiter(lines: Iterator[String]): Option[(TextFormatter, ResultSet)] = {
    // attempt to get up to 5 non-empty lines from the source file
    val sampleLines = lines.take(5).toList

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