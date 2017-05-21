package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.DataResource

/**
  * Represents an INSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class Insert(target: DataResource,
                  fields: Seq[Field],
                  source: Executable,
                  append: Boolean,
                  hints: Hints)
  extends Executable {

  override def execute(scope: Scope): ResultSet = {
    var count = 0L
    val outputSource = target.getOutputSource(append, hints)
      .getOrElse(throw new IllegalStateException(s"No device found for ${target.path}"))
    outputSource.open()
    source.execute(scope) foreach { data =>
      outputSource.write(data)
      count += 1
    }
    outputSource.close()
    Iterator(Seq("ROWS_INSERTED" -> count))
  }

}
