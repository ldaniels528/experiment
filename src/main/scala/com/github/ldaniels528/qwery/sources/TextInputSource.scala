package com.github.ldaniels528.qwery.sources

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import com.github.ldaniels528.qwery.formats.{JSONFormat, TextFormat}
import com.github.ldaniels528.qwery.ops.{ResultSet, Scope}

import scala.io.Source

/**
  * Text Input Source
  * @author lawrence.daniels@gmail.com
  */
class TextInputSource(lines: Iterator[String], formatter: TextFormat, sampleSet: Option[ResultSet] = None)
  extends QueryInputSource {

  override def execute(scope: Scope): ResultSet = {
    val preloaded = sampleSet map { results => Iterator(results.toSeq: _*) } getOrElse Iterator.empty
    preloaded ++ (lines map formatter.fromText)
  }
}

/**
  * Text Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object TextInputSource extends QueryInputSourceFactory {

  override def apply(uri: String): Option[TextInputSource] = uri match {
    case url if url.toLowerCase.startsWith("http://") | url.toLowerCase.startsWith("https://") =>
      TextInputSource.fromURL(url)
    case file if file.toLowerCase.endsWith(".json") =>
      Option(TextInputSource.fromFile(file, JSONFormat()))
    case file if file.toLowerCase.endsWith(".gz") =>
      TextInputSource.fromGzipFile(file)
    case file =>
      TextInputSource.fromFile(file)
  }

  override def understands(url: String): Boolean = url.toLowerCase() match {
    case s if s.startsWith("http://") | s.startsWith("https://") => true
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") | s.endsWith(".txt") => true
    case s if s.endsWith(".json") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

  def fromFile(file: String): Option[TextInputSource] = {
    lazy val lines = Source.fromFile(file).getLines()
    TextFormat.autodetectDelimiter(lines) map { case (formatter, sample) =>
      new TextInputSource(lines, formatter, sampleSet = Some(sample))
    }
  }

  def fromFile(file: String, formatter: TextFormat): TextInputSource = {
    new TextInputSource(Source.fromFile(file).getLines(), formatter)
  }

  def fromGzipFile(file: String): Option[TextInputSource] = {
    lazy val lines = Source.fromInputStream(new GZIPInputStream(new FileInputStream(file))).getLines()
    TextFormat.autodetectDelimiter(lines) map { case (formatter, sample) =>
      new TextInputSource(lines, formatter, sampleSet = Some(sample))
    }
  }

  def fromURL(url: String): Option[TextInputSource] = {
    lazy val lines = Source.fromURL(url).getLines()
    TextFormat.autodetectDelimiter(lines) map { case (formatter, sample) =>
      new TextInputSource(lines, formatter, sampleSet = Some(sample))
    }
  }

  def fromURL(url: String, formatter: TextFormat): TextInputSource = {
    new TextInputSource(Source.fromURL(url).getLines(), formatter)
  }

}
