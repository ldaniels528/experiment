package com.github.ldaniels528.qwery.sources

import java.io.{File, FileInputStream}
import java.net.URL
import java.util.zip.GZIPInputStream

import com.github.ldaniels528.qwery.ResultSet
import com.github.ldaniels528.qwery.ops.Scope
import com.github.ldaniels528.qwery.util.StringHelper._

import scala.io.{BufferedSource, Source}

/**
  * Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
class DelimitedInputSource(source: BufferedSource) extends QueryInputSource {
  private lazy val lines = source.getLines().filter(_.trim.nonEmpty)

  override def execute(scope: Scope): ResultSet = {
    autodetectDelimiter() match {
      case Some((delimiter, headers, rows)) =>
        lines.map(line => headers zip line.delimitedSplit(delimiter)) ++ rows.iterator
      case None =>
        Iterator.empty
    }
  }

  private def autodetectDelimiter(): Option[(Char, List[String], List[Seq[(String, String)]])] = {
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
    } yield (delimiter, headers, rows)
  }

}

/**
  * Delimited Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
object DelimitedInputSource extends QueryInputSourceFactory {

  def apply(uri: String): Option[DelimitedInputSource] = uri match {
    case url if url.toLowerCase.startsWith("http://") | url.toLowerCase.startsWith("https://") =>
      Option(URLDelimitedInputSource(new URL(url)))
    case file if file.toLowerCase.endsWith(".gz") =>
      Option(GzipFileDelimitedInputSource(new File(file)))
    case file =>
      Option(FileDelimitedInputSource(new File(file)))
  }

  def apply(file: File): DelimitedInputSource = FileDelimitedInputSource(file)

  def apply(url: URL): DelimitedInputSource = URLDelimitedInputSource(url)

  override def understands(url: String): Boolean = url.toLowerCase() match {
    case s if s.startsWith("http://") | s.startsWith("https://") => true
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") | s.endsWith(".txt") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

}

/**
  * File Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class FileDelimitedInputSource(file: File)
  extends DelimitedInputSource(Source.fromFile(file))

/**
  * Gzip'd File Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class GzipFileDelimitedInputSource(file: File)
  extends DelimitedInputSource(Source.fromInputStream(new GZIPInputStream(new FileInputStream(file))))

/**
  * URL Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class URLDelimitedInputSource(url: URL)
  extends DelimitedInputSource(Source.fromURL(url))