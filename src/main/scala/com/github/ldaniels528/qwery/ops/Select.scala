package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.sources.QueryInputSource

/**
  * Represents a selection query
  * @author lawrence.daniels@gmail.com
  */
case class Select(source: Option[QueryInputSource],
                  fields: Seq[Value],
                  condition: Option[Expression] = None,
                  groupFields: Option[Seq[Field]] = None,
                  sortFields: Option[Seq[(Field, Int)]] = None,
                  limit: Option[Int] = None)
  extends Executable {

  override def execute(scope: Scope): ResultSet = source match {
    case Some(device) =>
      val rows = device.execute(this)
        .map(row => LocalScope(scope, row))
        .filter(rowScope => condition.isEmpty || condition.exists(_.satisfies(rowScope)))
        .toIterator
        .take(limit getOrElse Int.MaxValue)

      if (fields.isAllFields) rows.map(_.data) else rows.map(filterRow)
    case None =>
      Iterator.empty
  }

  private def filterRow(scope: LocalScope): Row = {
    fields.flatMap(ev => ev.evaluate(scope).map(value => scope.getName(ev) -> value))
  }

}
