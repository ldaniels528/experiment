package com.github.ldaniels528.qwery.ops.sql

import com.github.ldaniels528.qwery.ops.{ResultSet, Scope, _}
import com.github.ldaniels528.qwery.sources.NamedResource

/**
  * Represents a JOIN operation
  * @author lawrence.daniels@gmail.com
  */
sealed trait Join {

  def join(left: Executable, scope: Scope): ResultSet

}

/**
  * Represents an INNER JOIN
  * @author lawrence.daniels@gmail.com
  */
case class InnerJoin(leftAlias: Option[String], right: NamedResource, condition: Condition) extends Join {

  def join(left: Executable, scope: Scope): ResultSet = {
    ResultSet(rows =
      left.execute(scope).rows flatMap { rowLeft =>
        right.execute(scope).rows flatMap { rowRight =>
          val combinedRow: Row =
            rowLeft.map { case (name, value) => leftAlias.map(alias => s"$alias.$name").getOrElse(name) -> value } ++
              rowRight.map { case (name, value) => s"${right.name}.$name" -> value }

          // determine whether to include the row
          if (condition.isSatisfied(Scope(scope, combinedRow))) Some(combinedRow) else None
        }
      })
  }
}