package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Hints, ResultSet, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

import scala.language.postfixOps

/**
  * Auto-Detecting Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class AutoDetectingDelimitedInputSource(device: InputDevice, hints: Option[Hints]) extends InputSource {
  private var delimiter: Char = _
  private var headers: Seq[String] = Nil
  private var buffer: List[Row] = Nil

  override def read(): Option[Row] = {
    if (headers.isEmpty) {
      autodetectDelimiter() match {
        case Some((_delimiter, _headers, rows)) =>
          this.delimiter = _delimiter
          this.headers = _headers
          this.buffer = rows.toList
        case None =>
          throw new IllegalStateException("The format could not be automatically detected")
      }
    }

    buffer match {
      case row :: _ => buffer = buffer.tail; Option(row)
      case Nil => device.read().map(rec => headers zip new String(rec.data).delimitedSplit(delimiter))
    }
  }

  override def open(): Unit = device.open()

  override def close(): Unit = device.close()

  private def autodetectDelimiter(): Option[(Char, List[String], ResultSet)] = {
    val sampleLines = (1 to 5).flatMap(_ => device.read()).map(r => new String(r.data))

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
      rows = sampleLines.tail.map(line => headers zip line.delimitedSplit(delimiter)) toIterator
    } yield (delimiter, headers, rows)
  }

}