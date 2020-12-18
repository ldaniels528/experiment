package com.qwery.models

import com.qwery.models.expressions.{Condition, Expression}

/**
 * Represents a SQL UPDATE statement
 * @param table   the [[TableRef table]] to update
 * @param changes the update assignments
 * @param where   the optional [[Condition where clause]]
 * @param limit   the limit
 * @author lawrence.daniels@gmail.com
 */
case class Update(table: TableRef,
                  changes: Seq[(String, Expression)],
                  where: Option[Condition],
                  limit: Option[Int])
  extends Invokable