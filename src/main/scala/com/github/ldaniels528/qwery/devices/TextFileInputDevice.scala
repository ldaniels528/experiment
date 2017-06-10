package com.github.ldaniels528.qwery.devices

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import com.github.ldaniels528.qwery.devices.DeviceHelper._
import com.github.ldaniels528.qwery.devices.InputDevice._
import com.github.ldaniels528.qwery.ops.{Hints, Scope}
import org.slf4j.LoggerFactory

import scala.io.{BufferedSource, Source}

/**
  * Text File Input Device
  * @author lawrence.daniels@gmail.com
  */
case class TextFileInputDevice(path: String, hints: Option[Hints]) extends InputDevice {
  private lazy val log = LoggerFactory.getLogger(getClass)
  private var reader: Option[BufferedSource] = None
  private var lines: Iterator[String] = Iterator.empty
  private var offset: Long = _

  override def close(): Unit = reader.foreach(_.close())

  override def getSize: Option[Long] = Some(new File(path).getSize)

  override def open(scope: Scope): Unit = {
    super.open(scope)
    val source = getSource(path)
    reader = Option(source)
    lines = source.getNonEmptyLines
    statsGen.fileSize = Some(new File(path).getSize)
    offset = 0L
  }

  override def read(): Option[Record] = {
    (if (lines.hasNext) Option(lines.next()).map(_.getBytes()) else None) map { bytes =>
      offset += 1
      statsGen.update(records = 1, bytesRead = bytes.length) foreach { stats =>
        log.info(stats.toString)
      }
      Record(bytes, offset)
    }
  }

  private def getSource(path: String) = path.toLowerCase match {
    case s if s.endsWith(".gz") | hints.exists(_.isGzip) => Source.fromInputStream(new GZIPInputStream(new FileInputStream(path)))
    case s if s.startsWith("http://") | s.startsWith("https://") => Source.fromURL(path)
    case _ => Source.fromFile(path)
  }

}

/**
  * Text File Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object TextFileInputDevice extends InputDeviceFactory with SourceUrlParser {

  /**
    * Returns a compatible input device for the given URL.
    * @param path the given URL (e.g. "./companylist.csv")
    * @return an option of the [[InputDevice input device]]
    */
  override def parseInputURL(path: String, hints: Option[Hints]): Option[InputDevice] = {
    path.toLowerCase() match {
      case _ if path.startsWith("file://") => Option(TextFileInputDevice(path.drop(7), hints))
      case uri if uri.startsWith("http://") | uri.startsWith("https://") => Option(TextFileInputDevice(path, hints))
      case _ if new File(path).exists() => Option(TextFileInputDevice(path, hints))
      case _ => None
    }
  }

}
