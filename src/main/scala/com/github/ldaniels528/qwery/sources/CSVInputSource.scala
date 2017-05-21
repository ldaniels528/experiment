package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.Row
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * CSV Input Source
  * @author lawrence.daniels@gmail.com
  */
case class CSVInputSource(device: InputDevice, delimiter: String = ",", quoted: Boolean = true, embeddedHeaders: Boolean = true)
  extends InputSource {
  private var headers: Seq[String] = Nil
  private val delimiterCh = delimiter.head

  override def open(): Unit = device.open()

  override def close(): Unit = device.close()

  override def read(): Option[Row] = {
    headers match {
      case h if h.isEmpty && embeddedHeaders =>
        for {
          headers <- device.read().map(r => parse(r.data))
          _ = this.headers = headers
          data <- device.read().map(r => parse(r.data))
        } yield headers zip data
      case h if h.isEmpty =>
        device.read() map { case Record(_, bytes) =>
          headers = parse(bytes).indices.map(n => s"field$n")
          headers zip parse(bytes)
        }
      case _ =>
        device.read() map { case Record(_, bytes) =>
          headers zip parse(bytes)
        }
    }
  }

  private def parse(bytes: Array[Byte]): List[String] = new String(bytes).delimitedSplit(delimiterCh)

}