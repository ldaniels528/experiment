package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.SourceUrlParser
import com.github.ldaniels528.qwery.ops.{Hints, Row}

/**
  * Output Source
  * @author lawrence.daniels@gmail.com
  */
trait OutputSource extends IOSource {

  def write(row: Row): Unit

}

/**
  * Output Source Companion
  * @author lawrence.daniels@gmail.com
  */
object OutputSource extends SourceUrlParser {

  def apply(path: String, hints: Option[Hints] = None): Option[OutputSource] = parseOutputSource(path, hints)

}