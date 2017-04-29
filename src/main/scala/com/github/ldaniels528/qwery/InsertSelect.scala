package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.sources.QueryOutputSource

/**
  * Represents an INSERT-SELECT statement
  * @author lawrence.daniels@gmail.com
  */
case class InsertSelect(target: QueryOutputSource, fields: Seq[Field], query: Query) extends Statement {

  override def execute(): TraversableOnce[Seq[(String, Any)]] = {
    val results = query.execute()
    var count = 0L
    results foreach { data =>
      target.write(data)
      count += 1
    }
    target.close()
    Seq(Seq("ROWS_INSERTED" -> count))
  }

}
