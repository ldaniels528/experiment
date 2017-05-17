package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.QueryResource

/**
  * Represents an INSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class Insert(target: QueryResource, fields: Seq[Field], source: Executable, hints: Hints)
  extends Executable {

  override def execute(scope: Scope): ResultSet = {
    var count = 0L
    val device = target.getOutputSource getOrElse (throw new IllegalStateException(s"No device found for ${target.path}"))
    device.open(hints)
    source.execute(scope) foreach { data =>
      device.write(data)
      count += 1
    }
    device.close()
    Seq(Seq("ROWS_INSERTED" -> count))
  }

}
