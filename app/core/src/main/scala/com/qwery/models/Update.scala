package com.qwery.models

import com.qwery.models.expressions.{Condition, Expression}

/**
  * Represents a SQL UPDATE statement
  * @param table       the [[TableRef table]] to update
  * @param assignments the update assignments
  * @param where       the optional [[Condition where clause]]
 *  @param limit       the limit
  * @author lawrence.daniels@gmail.com
  */
case class Update(table: TableRef,
                  assignments: Seq[(String, Expression)],
                  where: Option[Condition],
                  limit: Option[Int])
  extends Invokable