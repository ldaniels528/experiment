package com.qwery.models

import com.qwery.models.expressions.{AllFields, Condition, Expression, Field}

/**
  * Represents a SQL-like SELECT statement
  * @param fields  the given [[Expression columns]]
  * @param from    the given [[Invokable queryable resource]]
  * @param joins   the collection of [[Join join]] clauses
  * @param groupBy the columns by which to group
  * @param having  the aggregation condition
  * @param orderBy the columns by which to order
  * @param where   the optional [[Condition where clause]]
  * @param limit   the optional maximum number of results
  */
case class Select(fields: Seq[Expression] = Seq(AllFields),
                  from: Option[Invokable] = None,
                  joins: Seq[Join] = Nil,
                  groupBy: Seq[Field] = Nil,
                  having: Option[Condition] = None,
                  orderBy: Seq[OrderColumn] = Nil,
                  where: Option[Condition] = None,
                  limit: Option[Int] = None) extends Invokable with Aliasable
