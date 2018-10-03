package com.qwery.models

import com.qwery.models.JoinTypes.JoinType
import com.qwery.models.expressions.{Condition, Expression}

/**
  * Represents a SQL-like SELECT statement
  * @param fields  the given [[Expression columns]]
  * @param from    the given [[Invokable queryable resource]]
  * @param joins   the collection of [[Join join]] clauses
  * @param groupBy the columns by which to group
  * @param orderBy the columns by which to order
  * @param where   the optional [[Condition where clause]]
  * @param limit   the optional maximum number of results
  */
case class Select(fields: Seq[Expression],
                  from: Option[Invokable] = None,
                  joins: Seq[Join] = Nil,
                  groupBy: Seq[String] = Nil,
                  orderBy: Seq[OrderColumn] = Nil,
                  where: Option[Condition] = None,
                  limit: Option[Int] = None) extends Invokable with Aliasable

/**
  * Represents a JOIN clause
  * @param source     the [[TableRef table]] or [[Invokable query]]
  * @param condition the [[Condition conditional expression]]
  * @param `type`    the given [[JoinType]]
  */
case class Join(source: Invokable, condition: Condition, `type`: JoinType)

/**
  * Represents an enumeration of JOIN types
  * @author lawrence.daniels@gmail.com
  */
object JoinTypes extends Enumeration {
  type JoinType = Value
  val CROSS, INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER: JoinType = Value
}

/**
  * Order Column
  * @param name      the name of the column
  * @param ascending indicates whether the column is ascending (or conversly descending)
  */
case class OrderColumn(name: String, ascending: Boolean = true) extends Aliasable

/**
  * Represents a Union operation; which combines two queries.
  * @param query0 the first query
  * @param query1 the second query
  */
case class Union(query0: Invokable, query1: Invokable) extends Invokable with Aliasable