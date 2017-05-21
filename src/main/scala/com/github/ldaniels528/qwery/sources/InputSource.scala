package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.ops.{Executable, ResultSet, Row, Scope}

/**
  * Input Source
  * @author lawrence.daniels@gmail.com
  */
trait InputSource extends Executable {

  def close(): Unit

  override def execute(scope: Scope): ResultSet = toIterator

  def open(): Unit

  def read(): Option[Row]

  def toIterator: Iterator[Row] = new Iterator[Row] {
    open()

    private var nextRow_? : Option[Row] = read()

    override def hasNext: Boolean = nextRow_?.nonEmpty

    override def next(): Row = nextRow_? match {
      case Some(row) =>
        nextRow_? = read()
        if (nextRow_?.isEmpty) close()
        row
      case None =>
        throw new IllegalStateException("Empty iterator")
    }
  }

}

/**
  * Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
object InputSource extends InputSourceFactory {

  override def apply(path: String): Option[InputSource] = path.toLowerCase() match {
    case file if file.endsWith(".csv") => Option(CSVInputSource(TextFileInputDevice(path)))
    case file if file.endsWith(".json") => Option(JSONInputSource(TextFileInputDevice(path)))
    case file if file.endsWith(".psv") => Option(CSVInputSource(TextFileInputDevice(path), delimiter = "|"))
    case file if file.endsWith(".tsv") => Option(CSVInputSource(TextFileInputDevice(path), delimiter = "\t"))
    case file if file.endsWith(".txt") => Option(AutoDetectingInputSource(TextFileInputDevice(path)))
    case _ => Option(CSVInputSource(TextFileInputDevice(path)))
  }

  override def understands(path: String): Boolean = path.toLowerCase() match {
    case s if s.startsWith("http://") | s.startsWith("https://") => true
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") => true
    case s if s.endsWith(".txt") | s.endsWith(".json") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

}