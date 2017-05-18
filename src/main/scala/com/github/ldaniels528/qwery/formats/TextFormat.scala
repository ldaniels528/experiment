package com.github.ldaniels528.qwery.formats

import com.github.ldaniels528.qwery.ops.{ResultSet, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Text Format
  * @author lawrence.daniels@gmail.com
  */
trait TextFormat {

  def fromText(line: String): Row

  def toText(row: Row): Seq[String]

}

/**
  * Text Format Singleton
  * @author lawrence.daniels@gmail.com
  */
object TextFormat {

  def guessFormat(path: String): Option[TextFormat] = path.toLowerCase match {
    case file if file.endsWith(".csv") => Option(CSVFormat())
    case file if file.endsWith(".json") => Option(JSONFormat())
    case file if file.endsWith(".psv") => Option(CSVFormat(delimiter = "|"))
    case file if file.endsWith(".tsv") => Option(CSVFormat(delimiter = "\t"))
    case file if file.endsWith(".txt") => Option(CSVFormat())
    case _ => None
  }

  def understands(path: String): Boolean = path.toLowerCase() match {
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") => true
    case s if s.endsWith(".txt") | s.endsWith(".json") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

  def autodetectDelimiter(lines: Iterator[String]): Option[(TextFormat, ResultSet)] = {
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
    } yield (new CSVFormat(delimiter = String.valueOf(delimiter), headers = headers), rows)
  }

}