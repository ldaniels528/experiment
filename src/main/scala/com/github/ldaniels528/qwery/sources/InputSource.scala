package com.github.ldaniels528.qwery.sources

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
  * Input Source Factory
  * @author lawrence.daniels@gmail.com
  */
object InputSource extends InputSourceFactory with SourceUrlParser {

  /**
    * Determines the appropriate input source based on the given path and hints
    * @param path  the given path
    * @param hints the given [[Hints hints]]
    * @return an option of an [[InputSource input source]]
    */
  override def apply(path: String, hints: Option[Hints] = None): Option[InputSource] = parseInputSource(path, hints)

  override def understands(path: String): Boolean = path.toLowerCase() match {
    case s if s.matches("^\\S+:\\S+:.*") => true
    case s if s.startsWith("http://") | s.startsWith("https://") => true
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") => true
    case s if s.endsWith(".txt") | s.endsWith(".json") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

}