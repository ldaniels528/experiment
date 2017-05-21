package com.github.ldaniels528.qwery.sources

import java.text.DecimalFormat

import com.github.ldaniels528.qwery.ops.Row

/**
  * CSV Output Source
  * @author lawrence.daniels@gmail.com
  */
case class CSVOutputSource(device: OutputDevice, delimiter: String = ",", quoted: Boolean = true, embeddedHeaders: Boolean = true)
  extends OutputSource {
  private lazy val numberFormat = new DecimalFormat("###.#####")
  private var headersApplied = false
  private var offset = 0L

  override def write(row: Row): Unit = {
    // apply the headers?
    if (embeddedHeaders && !headersApplied) {
      headersApplied = true
      val headers = row.map(_._1).map(s => '"' + s + '"').mkString(delimiter)
      offset += 1
      device.write(Record(offset, headers.getBytes()))
    }

    // apply a line of data
    val line = row.map(_._2).map(_.asInstanceOf[Object]).map {
      case n: Number => numberFormat.format(n)
      case x => asString(x)
    } mkString delimiter
    offset += 1
    device.write(Record(offset, line.getBytes()))
  }

  override def open(): Unit = device.open()

  override def close(): Unit = device.close()

  private def asString(x: AnyRef) = if (quoted) '"' + x.toString + '"' else x.toString

}
