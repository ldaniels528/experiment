package com.github.ldaniels528.qwery.devices

import java.io.{BufferedWriter, FileWriter}

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Text File Output Device
  * @author lawrence.daniels@gmail.com
  */
case class TextFileOutputDevice(path: String, append: Boolean = false) extends OutputDevice {
  private var writer: Option[BufferedWriter] = None

  override def close(): Unit = writer.foreach(_.close())
  
  override def open(scope: Scope): Unit = writer = Option(new BufferedWriter(new FileWriter(path, append)))

  override def write(record: Record): Unit = {
    writer.foreach { out =>
      out.write(new String(record.data))
      out.newLine()
    }
  }

}