package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Hints, Row}

/**
  * Output Source
  * @author lawrence.daniels@gmail.com
  */
trait OutputSource {

  def close(): Unit

  def open(): Unit

  def write(row: Row): Unit

}

/**
  * Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
object OutputSource extends OutputSourceFactory {

  override def apply(path: String, append: Boolean, hints: Hints): Option[OutputSource] = path.toLowerCase() match {
    case file if file.endsWith(".csv") => Option(CSVOutputSource(TextFileOutputDevice(path, append)))
    case file if file.endsWith(".json") => Option(JSONOutputSource(TextFileOutputDevice(path, append)))
    case file if file.endsWith(".psv") => Option(CSVOutputSource(TextFileOutputDevice(path, append), delimiter = "|"))
    case file if file.endsWith(".tsv") => Option(CSVOutputSource(TextFileOutputDevice(path, append), delimiter = "\t"))
    case file if file.endsWith(".txt") => Option(CSVOutputSource(TextFileOutputDevice(path, append)))
    case _ => None
  }

  override def understands(path: String): Boolean = path.toLowerCase() match {
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") => true
    case s if s.endsWith(".txt") | s.endsWith(".json") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

}