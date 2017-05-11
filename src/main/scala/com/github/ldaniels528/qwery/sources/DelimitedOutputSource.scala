package com.github.ldaniels528.qwery.sources

import java.io.{BufferedWriter, File, FileWriter}
import java.net.URL

import com.github.ldaniels528.qwery.Row

/**
  * Delimited Output Source
  * @author lawrence.daniels@gmail.com
  */
abstract class DelimitedOutputSource(writer: BufferedWriter, delimiter: String = ",", quoted: Boolean = true)
  extends QueryOutputSource {
  private var headersWritten: Boolean = false

  override def close(): Unit = writer.close()

  override def flush(): Unit = writer.flush()

  override def write(data: Row): Unit = {
    if (!headersWritten) {
      headersWritten = true
      val header = data.map(_._1).map(s => '"' + s + '"').mkString(delimiter)
      writer.write(header)
      writer.newLine()
    }

    val line = data.map(_._2).map(_.asInstanceOf[AnyRef]).map {
      case n: Number => n.toString
      case x => asString(x)
    } mkString delimiter

    writer.write(line)
    writer.newLine()
  }

  private def asString(x: AnyRef) = if (quoted) '"' + x.toString + '"' else x.toString

}

/**
  * Delimited Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
object DelimitedOutputSource extends QueryOutputSourceFactory {

  def apply(uri: String): Option[DelimitedOutputSource] = uri match {
    case s if s.toLowerCase.startsWith("http://") | s.toLowerCase.startsWith("https://") => Option(apply(new URL(s)))
    case s if s.startsWith("/") | s.startsWith("./") => Option(apply(new File(s)))
    case _ => None
  }

  def apply(file: File): DelimitedOutputSource = FileDelimitedOutputSource(file)

  def apply(url: URL): DelimitedOutputSource = throw new IllegalStateException("URL output is not supported")

  override def understands(url: String): Boolean = url.startsWith("/") || url.startsWith("./")

}

/**
  * File Delimited Output Source
  * @author lawrence.daniels@gmail.com
  */
case class FileDelimitedOutputSource(file: File, delimiter: String = ",", quoted: Boolean = true, append: Boolean = false)
  extends DelimitedOutputSource(new BufferedWriter(new FileWriter(file, append)), delimiter = delimiter, quoted = quoted)
