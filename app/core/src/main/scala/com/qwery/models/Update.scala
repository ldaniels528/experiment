package com.qwery.models

import com.qwery.models.expressions.{Condition, Expression}

/**
  * Represents a SQL UPDATE statement
  * @param table       the given [[TableRef table]] to update
  * @param assignments the update assignments
  * @param where       the optional [[Condition where clause]]
  * @author lawrence.daniels@gmail.com
  */
case class Update(table: Invokable,
                  assignments: Seq[(String, Expression)],
                  where: Option[Condition])
  extends Invokable