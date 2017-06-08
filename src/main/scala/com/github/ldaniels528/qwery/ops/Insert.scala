package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.DataResource
import com.github.ldaniels528.qwery.util.ResourceHelper._

/**
  * Represents an INSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class Insert(target: DataResource, fields: Seq[Field], source: Executable) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    var count = 0L
    val outputSource = target.getOutputSource(scope)
      .getOrElse(throw new IllegalStateException(s"No device found for ${target.path}"))
    outputSource.open(scope)
    outputSource use { device =>
      source.execute(scope) foreach { row =>
        // TODO perform filtering of the row?
        device.write(row)
        count += 1
      }
    }
    ResultSet(rows = Iterator(Seq("ROWS_INSERTED" -> count)), statistics = outputSource.getStatistics)
  }

}
