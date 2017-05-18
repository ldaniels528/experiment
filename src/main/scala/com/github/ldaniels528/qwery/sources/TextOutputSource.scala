package com.github.ldaniels528.qwery.sources

import java.io.{BufferedWriter, FileOutputStream, FileWriter, OutputStreamWriter}
import java.util.zip.GZIPOutputStream

import com.github.ldaniels528.qwery.formats.TextFormat
import com.github.ldaniels528.qwery.ops.{Hints, Row}

/**
  * Text Output Source
  * @author lawrence.daniels@gmail.com
  */
class TextOutputSource(writer: BufferedWriter, formatter: TextFormat) extends QueryOutputSource {
  private var hints = Hints()

  override def open(hints: Hints): Unit = this.hints = hints

  override def close(): Unit = writer.close()

  override def flush(): Unit = writer.flush()

  override def write(row: Row): Unit = {
    formatter.toText(row) foreach { line =>
      writer.write(line)
      writer.newLine()
    }
  }

}

/**
  * Text Output Source Companion
  * @author lawrence.daniels@gmail.com
  */
object TextOutputSource extends QueryOutputSourceFactory {

  override def apply(path: String, append: Boolean): Option[TextOutputSource] = path match {
    case s if s.toLowerCase.startsWith("http://") | s.toLowerCase.startsWith("https://") =>
      TextFormat.guessFormat(s).map(fromUrl(s, _))
    case file if file.toLowerCase.endsWith(".gz") =>
      TextFormat.guessFormat(file.dropRight(3)).map(fromFile(file, _, append, compress = true))
    case file => TextFormat.guessFormat(file).map(fromFile(file, _, append))
  }

  def fromFile(file: String,
               formatter: TextFormat,
               append: Boolean,
               compress: Boolean = false): TextOutputSource = {
    val writer = if (compress)
      new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file))))
    else
      new BufferedWriter(new FileWriter(file, append))
    new TextOutputSource(writer, formatter)
  }

  def fromUrl(url: String, formatter: TextFormat): TextOutputSource = {
    throw new IllegalStateException("HTTP output is not yet supported")
  }

  override def understands(path: String): Boolean = path.toLowerCase() match {
    case s if s.startsWith("http://") | s.startsWith("https://") => true
    case _ => TextFormat.understands(path)
  }

}