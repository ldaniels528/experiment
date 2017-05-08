package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.util.StringHelper._
import java.io.File
import java.net.URL

import com.github.ldaniels528.qwery.ops.Query

import scala.io.{BufferedSource, Source}

/**
  * Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
abstract class DelimitedInputSource(source: BufferedSource) extends QueryInputSource {
  private lazy val lines = source.getLines().filter(_.trim.nonEmpty)

  override def execute(query: Query): TraversableOnce[Map[String, String]] = {
    val results = autodetectDelimiter() match {
      case Some((delimiter, headers, rows)) =>
        lines.map(line => Map(headers zip line.delimitedSplit(delimiter): _*)) ++ rows.iterator
      case None =>
        Iterator.empty
    }

    results
      .filter(r => query.condition.isEmpty || query.condition.exists(_.satisfies(r)))
      .take(query.limit getOrElse Int.MaxValue)
  }

  private def autodetectDelimiter(): Option[(Char, List[String], List[Map[String, String]])] = {
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
      rows = sampleLines.tail.map(line => Map(headers zip line.delimitedSplit(delimiter): _*))
    } yield (delimiter, headers, rows)
  }

}

/**
  * Delimited Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
object DelimitedInputSource extends QueryInputSourceFactory {

  def apply(uri: String): Option[DelimitedInputSource] = uri match {
    case s if s.toLowerCase.startsWith("http://") | s.toLowerCase.startsWith("https://") => Option(apply(new URL(s)))
    case s if s.startsWith("/") | s.startsWith("./") => Option(apply(new File(s)))
    case _ => None
  }

  def apply(file: File): DelimitedInputSource = FileDelimitedInputSource(file)

  def apply(url: URL): DelimitedInputSource = URLDelimitedInputSource(url)

  override def understands(url: String): Boolean = url.startsWith("/") || url.startsWith("./")

}

/**
  * File Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class FileDelimitedInputSource(file: File) extends DelimitedInputSource(Source.fromFile(file))

/**
  * URL Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class URLDelimitedInputSource(url: URL) extends DelimitedInputSource(Source.fromURL(url))