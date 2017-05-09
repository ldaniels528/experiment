package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.sources.QueryOutputSource
import com.github.ldaniels528.qwery.ResultSet

/**
  * Represents an INSERT-SELECT statement
  * @author lawrence.daniels@gmail.com
  */
case class Insert(target: QueryOutputSource, fields: Seq[Field], source: Executable) extends Statement {

  override def execute(scope: Scope): ResultSet = {
    var count = 0L
    source.execute(scope) foreach { data =>
      target.write(data)
      count += 1
    }
    target.close()
    Seq(Seq("ROWS_INSERTED" -> count))
  }

}
