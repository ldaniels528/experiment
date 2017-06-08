package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.{InputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, ResultSet, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

import scala.language.postfixOps

/**
  * Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class DelimitedInputSource(device: InputDevice, hints: Option[Hints])
  extends InputSource {
  private var headers: Seq[String] = Nil
  private var delimiter = hints.flatMap(_.delimiter).flatMap(_.headOption).getOrElse(',')
  private var buffer: List[Row] = Nil

  override def read(): Option[Row] = {
    // have the headers been set?
    if (headers.isEmpty) setHeaders()

    // read the next row
    buffer match {
      case Nil => readNext()
      case row :: remaining => buffer = remaining; Option(row)
    }
  }

  @inline
  private def readNext(): Option[Row] = device.read() map { case Record(bytes, _, _) =>
    headers zip parse(bytes)
  }

  @inline
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
    } yield (delimiter, headers, ResultSet(rows))
  }

  @inline
  private def parse(bytes: Array[Byte]): List[String] = new String(bytes).delimitedSplit(delimiter)

  @inline
  private def setHeaders() = hints match {
    case h if h.flatMap(_.delimiter).isEmpty =>
      autodetectDelimiter() match {
        case Some((_delimiter, _headers, rows)) =>
          this.delimiter = _delimiter
          this.headers = _headers
          this.buffer = rows.toList
        case None =>
          throw new IllegalStateException("The delimiter could not be automatically detected")
      }
    case h if h.flatMap(_.headers).contains(true) =>
      device.read().map(r => parse(r.data)) foreach (headers => this.headers = headers)
    case _ =>
      val row_? = device.read() map { case Record(bytes, _, _) =>
        headers = parse(bytes).indices.map(n => s"field$n")
        headers zip parse(bytes)
      }
      row_?.foreach(row => buffer = row :: buffer)
  }

}

/**
  * Delimited Input Source Singleton
  * @author lawrence.daniels@gmail.com
  */
object DelimitedInputSource extends InputSourceFactory {

  override def findInputSource(device: InputDevice, hints: Option[Hints]): Option[InputSource] = {
    if (hints.exists(_.delimiter.nonEmpty)) Option(DelimitedInputSource(device, hints)) else None
  }

}