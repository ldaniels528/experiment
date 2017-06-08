package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.SourceUrlParser
import com.github.ldaniels528.qwery.ops.{Executable, Hints, ResultSet, Row, Scope}

/**
  * Input Source
  * @author lawrence.daniels@gmail.com
  */
trait InputSource extends IOSource with Executable {

  override def execute(scope: Scope): ResultSet = {
    open(scope)
    ResultSet(rows = toIterator, statistics = getStatistics)
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
  * Input Source Companion
  * @author lawrence.daniels@gmail.com
  */
object InputSource extends SourceUrlParser {

  def apply(path: String, hints: Option[Hints] = None): Option[InputSource] = parseInputSource(path, hints)

}