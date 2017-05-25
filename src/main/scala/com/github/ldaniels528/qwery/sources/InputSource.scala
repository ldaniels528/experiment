package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.TextFileInputDevice
import com.github.ldaniels528.qwery.ops.{Executable, Hints, ResultSet, Row, Scope}
import com.github.ldaniels528.qwery.util.OptionHelper.Risky._
import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Input Source
  * @author lawrence.daniels@gmail.com
  */
trait InputSource extends IOSource with Executable {

  override def execute(scope: Scope): ResultSet = {
    open(scope)
    toIterator
  }

  def read(): Option[Row]

  def toIterator: Iterator[Row] = new Iterator[Row] {

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

  override def apply(path: String, hints: Option[Hints] = None): Option[InputSource] = {
    (hints ?? Hints()) map {
      case hint if hint.isJson.contains(true) => JSONInputSource(TextFileInputDevice(path), hint)
      case hint if hint.delimiter.nonEmpty => DelimitedInputSource(TextFileInputDevice(path), hint)
      case hint =>
        path.toLowerCase() match {
          case uri if uri.startsWith("kafka:avro://") => AvroInputSource.parseURI(path, hint)
          case file if file.endsWith(".csv") => DelimitedInputSource(TextFileInputDevice(path), hints = hint.map(_.asCSV))
          case file if file.endsWith(".json") => JSONInputSource(TextFileInputDevice(path), hints = hint.map(_.asJSON))
          case file if file.endsWith(".psv") => DelimitedInputSource(TextFileInputDevice(path), hints = hint.map(_.asPSV))
          case file if file.endsWith(".tsv") => DelimitedInputSource(TextFileInputDevice(path), hints = hint.map(_.asTSV))
          case _ => DelimitedInputSource(TextFileInputDevice(path), hint)
        }
    }
  }

  override def understands(path: String): Boolean = path.toLowerCase() match {
    case s if s.startsWith("http://") | s.startsWith("https://") => true
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") => true
    case s if s.endsWith(".txt") | s.endsWith(".json") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

}