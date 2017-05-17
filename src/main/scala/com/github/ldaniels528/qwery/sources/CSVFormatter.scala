package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.util.StringHelper._
import com.github.ldaniels528.qwery.ops.{Hints, Row}

/**
  * CSV Formatter
  * @author lawrence.daniels@gmail.com
  */
case class CSVFormatter(delimiter: String = ",", headers: List[String] = Nil, quoted: Boolean = true)
  extends TextFormatter {
  private var headersApplied = false
  private val delimiterCh = delimiter.head

  override def fromText(line: String): Row = {
    headers zip line.delimitedSplit(delimiterCh)
  }

  override def toText(row: Row): Seq[String] = {
    var lines: List[String] = Nil

    // apply the headers
    if (headers.nonEmpty && !headersApplied) {
      headersApplied = true
      val header = row.map(_._1).map(s => '"' + s + '"').mkString(delimiter)
      lines = lines ::: header :: Nil
    }

    // apply a line of data
    val data = row.map(_._2).map(_.asInstanceOf[Object]).map {
      case n: Number => n.toString
      case x => asString(x)
    } mkString delimiter
    lines = lines ::: data :: Nil
    lines
  }

  private def asString(x: AnyRef) = if (quoted) '"' + x.toString + '"' else x.toString

}

/**
  * CSV Formatter Singleton
  * @author lawrence.daniels@gmail.com
  */
object CSVFormatter {

  def apply(hints: Hints): CSVFormatter = {
    new CSVFormatter(delimiter = hints.delimiter, quoted = hints.quoted)
  }

}