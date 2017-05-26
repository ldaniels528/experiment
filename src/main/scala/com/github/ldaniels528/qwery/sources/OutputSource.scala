package com.github.ldaniels528.qwery.sources

import com.github.ldaniels528.qwery.devices.TextFileOutputDevice
import com.github.ldaniels528.qwery.ops.{Hints, Row}
import com.github.ldaniels528.qwery.util.OptionHelper.Risky._
import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Output Source
  * @author lawrence.daniels@gmail.com
  */
trait OutputSource extends IOSource {

  def write(row: Row): Unit

}

/**
  * Output Source Factory
  * @author lawrence.daniels@gmail.com
  */
object OutputSource extends OutputSourceFactory {

  override def apply(path: String, append: Boolean, hints: Option[Hints]): Option[OutputSource] = {
    (hints ?? Hints()) map { hint =>
      val compress = hint.gzip.contains(true)
      path.toLowerCase() match {
        case file if file.endsWith(".csv") => DelimitedOutputSource(TextFileOutputDevice(path, append, compress), hint.asCSV)
        case file if file.endsWith(".json") => JSONOutputSource(TextFileOutputDevice(path, append, compress), hint.asJSON)
        case file if file.endsWith(".psv") => DelimitedOutputSource(TextFileOutputDevice(path, append, compress), hint.asPSV)
        case file if file.endsWith(".tsv") => DelimitedOutputSource(TextFileOutputDevice(path, append, compress), hint.asTSV)
        case file => DelimitedOutputSource(TextFileOutputDevice(path, append, compress), hint.asCSV)
      }
    }
  }

  override def understands(path: String): Boolean = path.toLowerCase() match {
    case s if s.endsWith(".csv") | s.endsWith(".psv") | s.endsWith(".tsv") => true
    case s if s.endsWith(".txt") | s.endsWith(".json") => true
    case s if s.endsWith(".gz") => understands(s.dropRight(3))
    case _ => false
  }

}