package com.qwery.models

import com.qwery.models.expressions.{AllFields, Condition, Expression, FieldRef}

/**
  * Represents a queryable entity
  * @author lawrence.daniels@gmail.com
  */
trait Queryable extends Invokable with Aliasable

/**
  * Represents a Subtraction operation; which returns the intersection of two queries.
  * @param query0 the first [[Queryable queryable resource]]
  * @param query1 the second [[Queryable queryable resource]]
  */
case class Except(query0: Invokable, query1: Invokable) extends Queryable

/**
  * Represents a Intersection operation.
  * @param query0 the first [[Queryable queryable resource]]
  * @param query1 the second [[Queryable queryable resource]]
  */
case class Intersect(query0: Invokable, query1: Invokable) extends Queryable

/**
  * Invokes a procedure by name
  * @param name the name of the procedure
  * @param args the collection of [[Expression arguments]] to be passed to the procedure upon invocation
  * @example {{{ CALL getEligibleFeeds("/home/ubuntu/feeds/") }}}
  */
case class ProcedureCall(name: String, args: List[Expression]) extends Queryable

/**
  * Represents a SQL-like SELECT statement
  * @param fields  the given [[Expression columns]]
  * @param from    the given [[Queryable queryable resource]]
  * @param joins   the collection of [[Join join]] clauses
  * @param groupBy the columns by which to group
  * @param having  the aggregation condition
  * @param orderBy the columns by which to order
  * @param where   the optional [[Condition where clause]]
  * @param limit   the optional maximum number of results
  */
case class Select(fields: Seq[Expression] = Seq(AllFields),
                  from: Option[Queryable] = None,
                  joins: Seq[Join] = Nil,
                  groupBy: Seq[FieldRef] = Nil,
                  having: Option[Condition] = None,
                  orderBy: Seq[OrderColumn] = Nil,
                  where: Option[Condition] = None,
                  limit: Option[Int] = None) extends Queryable

/**
  * Represents a Union operation; which combines two queries.
  * @param query0     the first [[Queryable queryable resource]]
  * @param query1     the second [[Queryable queryable resource]]
  * @param isDistinct indicates wthether the results should be distinct
  */
case class Union(query0: Invokable, query1: Invokable, isDistinct: Boolean = false) extends Queryable