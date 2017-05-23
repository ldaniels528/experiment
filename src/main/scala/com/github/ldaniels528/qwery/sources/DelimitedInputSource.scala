package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Hints, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Delimited Text Input Source
  * @author lawrence.daniels@gmail.com
  */
case class DelimitedInputSource(device: InputDevice, hints: Option[Hints])
  extends InputSource {
  private var headers: Seq[String] = Nil
  private val delimiterCh = hints.flatMap(_.delimiter).flatMap(_.headOption).getOrElse(',')

  override def open(): Unit = device.open()

  override def close(): Unit = device.close()

  override def read(): Option[Row] = {
    headers match {
      case h if h.isEmpty && hints.flatMap(_.headers).contains(true) =>
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