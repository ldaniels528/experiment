package com.github.ldaniels528.qwery.sources

import java.io.File
import java.net.URL

import com.github.ldaniels528.qwery.Query

import scala.io.{BufferedSource, Source}

/**
  * Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class DelimitedInputSource(source: BufferedSource) extends QueryInputSource {
  private lazy val lines = source.getLines().filter(_.trim.nonEmpty)

  override def execute(query: Query): TraversableOnce[Map[String, String]] = {
    val results = autodetectDelimiter() match {
      case Some((delimiter, headers, rows)) =>
        lines.map(line => Map(headers zip line.split(delimiter): _*)) ++ rows.iterator
      case None =>
        Iterator.empty
    }

    results
      .filter(r => query.condition.isEmpty || query.condition.exists(_.satisfies(r)))
      .take(query.limit getOrElse Int.MaxValue)
  }

  private def autodetectDelimiter(): Option[(Char, Array[String], List[Map[String, String]])] = {
    // attempt to get up to 5 non-empty lines from the source file
    val sampleLines = lines.take(5).toList

    // identify the potential delimiters (from the header line)
    val delimiters = sampleLines.headOption map { header =>
      header.collect {
        case c if !c.isLetterOrDigit => c
      }.distinct
    } map (_.toCharArray.toList) getOrElse Nil

    // find a delimiter where splitting all lines results in the same number of elements
    val delimiter_? = delimiters.find { delimiter =>
      sampleLines.headOption.map(_.split(delimiter).length).exists { length =>
        sampleLines.forall(_.split(delimiter).length == length)
      }
    }

    for {
      delimiter <- delimiter_?
      headers <- sampleLines.headOption.map(_.split(delimiter))
      rows = sampleLines.tail.map(line => Map(headers zip line.split(delimiter): _*))
    } yield (delimiter, headers, rows)
  }

}

/**
  * Delimited Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object DelimitedInputSource {

  def apply(uri: String): DelimitedInputSource = {
    DelimitedInputSource(uri match {
      case url if url.toLowerCase.startsWith("http://") | url.toLowerCase.startsWith("http://") => Source.fromURL(url)
      case file => Source.fromFile(file)
    })
  }

  def apply(file: File): DelimitedInputSource = DelimitedInputSource(Source.fromFile(file))

  def apply(url: URL): DelimitedInputSource = DelimitedInputSource(Source.fromURL(url))

}