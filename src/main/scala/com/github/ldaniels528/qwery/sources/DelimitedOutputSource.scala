package com.github.ldaniels528.qwery.sources

import java.io._
import java.net.URL
import java.util.zip.GZIPOutputStream

import com.github.ldaniels528.qwery.ops.{Hints, Row}

/**
  * Delimited Output Source
  * @author lawrence.daniels@gmail.com
  */
abstract class DelimitedOutputSource(output: Hints => BufferedWriter) extends QueryOutputSource {
  private var headersWritten = false
  private var hints = Hints()
  private var writer: BufferedWriter = _

  override def open(hints: Hints): Unit = {
    this.hints = hints
    writer = output(hints)
  }

  override def close(): Unit = writer.close()

  override def flush(): Unit = writer.flush()

  override def write(data: Row): Unit = {
    if (hints.headers && !headersWritten) {
      headersWritten = true
      val header = data.map(_._1).map(s => '"' + s + '"').mkString(hints.delimiter)
      writer.write(header)
      writer.newLine()
    }

    val line = data.map(_._2).map(_.asInstanceOf[Object]).map {
      case n: Number => n.toString
      case x => asString(x)
    } mkString hints.delimiter

    writer.write(line)
    writer.newLine()
  }

  private def asString(x: AnyRef) = if (hints.quoted) '"' + x.toString + '"' else x.toString

}

/**
  * Delimited Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
object DelimitedOutputSource extends QueryOutputSourceFactory {

  def apply(uri: String): Option[DelimitedOutputSource] = uri match {
    case s if s.toLowerCase.startsWith("http://") | s.toLowerCase.startsWith("https://") =>
      Option(apply(new URL(s)))
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") | s.endsWith(".txt") =>
      Option(FileDelimitedOutputSource(s))
    case file if file.toLowerCase.endsWith(".gz") =>
      Option(GzipFileDelimitedOutputSource(file))
    case _ => None
  }

  def apply(file: File): DelimitedOutputSource = FileDelimitedOutputSource(file.getCanonicalPath)

  def apply(url: URL): DelimitedOutputSource = throw new IllegalStateException("URL output is not supported")

  override def understands(url: String): Boolean = url.toLowerCase() match {
    case s if s.startsWith("http://") | s.startsWith("https://") => true
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") | s.endsWith(".txt") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

}

/**
  * File Delimited Output Source
  * @author lawrence.daniels@gmail.com
  */
case class FileDelimitedOutputSource(file: String)
  extends DelimitedOutputSource((hints: Hints) => new BufferedWriter(new FileWriter(file, hints.append)))

/**
  * Gzip'd File Delimited Input Source
  * @author lawrence.daniels@gmail.com
  */
case class GzipFileDelimitedOutputSource(file: String)
  extends DelimitedOutputSource((hints: Hints) => new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)))))