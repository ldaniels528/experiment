package com.github.ldaniels528.qwery.formats

import java.text.DecimalFormat

import com.github.ldaniels528.qwery.ops.{Hints, Row}
import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * CSV Format
  * @author lawrence.daniels@gmail.com
  */
case class CSVFormat(delimiter: String = ",",
                     headers: List[String] = Nil,
                     quoted: Boolean = true)
  extends TextFormat {
  private lazy val numberFormat = new DecimalFormat("###.#####")
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
      case n: Number => numberFormat.format(n)
      case x => asString(x)
    } mkString delimiter
    lines = lines ::: data :: Nil
    lines
  }

  private def asString(x: AnyRef) = if (quoted) '"' + x.toString + '"' else x.toString

}

/**
  * CSV Format Singleton
  * @author lawrence.daniels@gmail.com
  */
object CSVFormat {

  def apply(hints: Hints): CSVFormat = {
    new CSVFormat(delimiter = hints.delimiter, quoted = hints.quoted)
  }

}