package com.github.ldaniels528.qwery.sources

import java.io.FileInputStream

import scala.io.{BufferedSource, Source}

/**
  * Text File Input Device
  * @author lawrence.daniels@gmail.com
  */
case class TextFileInputDevice(path: String) extends InputDevice {
  private var reader: Option[BufferedSource] = None
  private var lines: Iterator[String] = Iterator.empty
  private var offset = 0L

  override def close(): Unit = reader.foreach(_.close())

  override def open(): Unit = {
    val source = getSource(path)
    reader = Option(source)
    lines = source.getLines().filter(_.trim.nonEmpty)
  }

  override def read(): Option[Record] = {
    (if (lines.hasNext) Some(lines.next()) else None) map { line =>
      offset += 1
      Record(offset, line.getBytes())
    }
  }

  private def getSource(path: String) = path.toLowerCase match {
    case s if s.endsWith(".gz") => Source.fromInputStream(new FileInputStream(path))
    case s if s.startsWith("http://") | s.startsWith("https://") => Source.fromURL(path)
    case _ => Source.fromFile(path)
  }

}

