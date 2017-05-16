package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.QueryOutputSource

/**
  * Represents an INSERT statement
  * @author lawrence.daniels@gmail.com
  */
case class Insert(target: QueryOutputSource, fields: Seq[Field], source: Executable, hints: Hints)
  extends Executable {

  override def execute(scope: Scope): ResultSet = {
    var count = 0L
    target.open(hints)
    source.execute(scope) foreach { data =>
      target.write(data)
      count += 1
    }
    target.close()
    Seq(Seq("ROWS_INSERTED" -> count))
  }

}
