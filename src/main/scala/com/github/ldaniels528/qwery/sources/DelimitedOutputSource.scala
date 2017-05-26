package com.github.ldaniels528.qwery.sources

import java.text.DecimalFormat

import com.github.ldaniels528.qwery.devices.{OutputDevice, Record}
import com.github.ldaniels528.qwery.ops.{Hints, Row}

/**
  * Delimited Output Source
  * @author lawrence.daniels@gmail.com
  */
case class DelimitedOutputSource(device: OutputDevice, hints: Option[Hints] = Some(Hints().asCSV))
  extends OutputSource {
  private lazy val numberFormat = new DecimalFormat("###.#####")
  private val delimiter = hints.flatMap(_.delimiter).getOrElse(",")
  private val applyHeaders = hints.exists(_.headers.contains(true))
  private val quotedText = hints.exists(_.quotedText.contains(true))
  private val quotedNumbers = hints.exists(_.quotedNumbers.contains(true))
  private var headersApplied = false
  private var offset = 0L

  override def write(row: Row): Unit = {
    // apply the headers?
    if (applyHeaders && !headersApplied) {
      headersApplied = true
      val headers = row.map(_._1).map(s => '"' + s + '"').mkString(delimiter)
      offset += 1
      device.write(Record(headers.getBytes(), offset))
    }

    // apply a line of data
    val line = row.map(_._2).map(_.asInstanceOf[Object]).map {
      case n: Number if quotedNumbers => quoted(numberFormat.format(n))
      case n: Number => numberFormat.format(n)
      case x => asString(x)
    } mkString delimiter
    offset += 1
    device.write(Record(line.getBytes(), offset))
  }

  private def asString(x: AnyRef) = if (quotedText) quoted(x.toString) else x.toString

  private def quoted(s: String) = {
    new StringBuilder(s.length + 2).append('\'').append(s).append('\'').toString()
  }

}
