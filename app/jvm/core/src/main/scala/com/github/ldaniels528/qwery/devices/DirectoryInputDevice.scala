package com.github.ldaniels528.qwery.devices

import java.io.File

import com.github.ldaniels528.qwery.devices.DirectoryInputDevice.DirectoryContents
import com.github.ldaniels528.qwery.devices.InputDevice._
import com.github.ldaniels528.qwery.ops.Hints
import com.github.ldaniels528.qwery.util.FileHelper._
import org.apache.commons.io.filefilter.WildcardFileFilter

import scala.io.Source

/**
  * Directory Input Device
  * @author lawrence.daniels@gmail.com
  */
case class DirectoryInputDevice(path: String, hints: Option[Hints]) extends InputDevice {
  private val files = getFileList
  private val contents = new DirectoryContents(files)

  override def getSize: Option[Long] = None //Option(files.map(_.length).sum)

  override def read(): Option[Record] = if (contents.hasNext) Option(contents.next()) else None

  override def close(): Unit = ()

  private def getFileList = {
    if (path.contains('*')) {
      val pathPcs = new File(path).getAbsolutePath.split(File.separatorChar)
      pathPcs.indexWhere(_.contains('*')) match {
        case index =>
          val pathSect = pathPcs.slice(0, index).mkString(File.separator)
          val patternSect = pathPcs.slice(index, pathPcs.length).mkString(File.separator)
          getFiles(new File(pathSect), new WildcardFileFilter(patternSect))
      }
    }
    else getFiles(new File(path))
  }

}

/**
  * Directory Input Device Companion
  * @author lawrence.daniels@gmail.com
  */
object DirectoryInputDevice {

  class DirectoryContents(var files: Stream[File]) extends Iterator[Record] {
    private var lines: Iterator[String] = Iterator.empty
    private var offset = 0L

    override def hasNext: Boolean = lines.hasNext || getNextSource.hasNext

    override def next(): Record = {
      if (lines.hasNext) {
        offset += 1
        Record(data = lines.next().getBytes(), offset)
      }
      else throw new IllegalStateException("Empty iterator")
    }

    private def getNextSource: Iterator[String] = {
      files.headOption match {
        case Some(file) =>
          files = files.tail
          val theLines = Source.fromFile(file).getNonEmptyLines
          lines = if (!theLines.hasNext && files.nonEmpty) getNextSource else theLines
          lines
        case None =>
          Iterator.empty
      }
    }

  }

}