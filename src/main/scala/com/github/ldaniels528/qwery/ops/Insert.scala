package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.DataResource
import com.github.ldaniels528.qwery.util.ResourceHelper._

/**
  * Represents an INSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class Insert(target: DataResource,
                  fields: Seq[Field],
                  source: Executable,
                  append: Boolean = false)
  extends Executable {

  override def execute(scope: Scope): ResultSet = {
    var count = 0L
    val outputSource = target.getOutputSource(append)
      .getOrElse(throw new IllegalStateException(s"No device found for ${target.path}"))
    outputSource manage { device =>
      source.execute(scope) foreach { data =>
        device.write(data)
        count += 1
      }
    }
    Iterator(Seq("ROWS_INSERTED" -> count))
  }

}
