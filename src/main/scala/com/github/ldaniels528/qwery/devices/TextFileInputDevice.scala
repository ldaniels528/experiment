package com.github.ldaniels528.qwery.devices

import java.io.FileInputStream
import java.util.zip.GZIPInputStream

import com.github.ldaniels528.qwery.devices.InputDevice._
import com.github.ldaniels528.qwery.ops.Scope

import scala.io.{BufferedSource, Source}

/**
  * Text File Input Device
  * @author lawrence.daniels@gmail.com
  */
case class TextFileInputDevice(path: String) extends InputDevice {
  private var reader: Option[BufferedSource] = None
  private var lines: Iterator[String] = Iterator.empty
  private var offset: Long = _

  override def close(): Unit = reader.foreach(_.close())

  override def open(scope: Scope): Unit = {
    val source = getSource(path)
    reader = Option(source)
    lines = source.getNonEmptyLines
    offset = 0L
  }

  override def read(): Option[Record] = {
    (if (lines.hasNext) Some(lines.next()) else None) map { line =>
      offset += 1
      Record(offset, line.getBytes())
    }
  }

  private def getSource(path: String) = path.toLowerCase match {
    case s if s.endsWith(".gz") => Source.fromInputStream(new GZIPInputStream(new FileInputStream(path)))
    case s if s.startsWith("http://") | s.startsWith("https://") => Source.fromURL(path)
    case _ => Source.fromFile(path)
  }

}

