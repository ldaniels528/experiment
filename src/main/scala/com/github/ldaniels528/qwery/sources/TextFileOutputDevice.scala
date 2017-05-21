package com.github.ldaniels528.qwery.sources

import java.io.{BufferedWriter, FileWriter}

/**
  * Text File Output Device
  * @author lawrence.daniels@gmail.com
  */
case class TextFileOutputDevice(path: String, append: Boolean = false) extends OutputDevice {
  private var writer: Option[BufferedWriter] = None

  override def close(): Unit = writer.foreach(_.close())
  
  override def open(): Unit = writer = Option(new BufferedWriter(new FileWriter(path, append)))

  override def write(record: Record): Unit = {
    writer.foreach { out =>
      out.write(new String(record.data))
      out.newLine()
    }
  }

}