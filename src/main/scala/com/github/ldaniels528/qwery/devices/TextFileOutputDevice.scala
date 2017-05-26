package com.github.ldaniels528.qwery.devices

import java.io._
import java.util.zip.GZIPOutputStream

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Text File Output Device
  * @author lawrence.daniels@gmail.com
  */
case class TextFileOutputDevice(path: String,
                                append: Boolean = false,
                                compress: Boolean = false) extends OutputDevice {
  private var writer: Option[BufferedWriter] = None

  override def close(): Unit = writer.foreach(_.close())

  override def open(scope: Scope): Unit = writer = Option(getSource(path))

  override def write(record: Record): Unit = writer.foreach { out =>
    statsGen.update(records = 1, bytesRead = record.data.length)
    out.write(new String(record.data))
    out.newLine()
  }

  private def getSource(path: String) = path.toLowerCase match {
    case s if compress | s.endsWith(".gz") =>
      new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(path, append))))
    case _ =>
      new BufferedWriter(new FileWriter(path, append))
  }

}