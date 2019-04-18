package com.qwery.models

import com.qwery.models.expressions.{Condition, Expression, Field}

/**
  * Window-Over clause
  * @param expression  the host [[Expression]]
  * @param partitionBy the given partition by columns
  * @param orderBy     the given order by columns
  * @param range       the RANGE BETWEEN clause
  * @example
  * {{{
  * SELECT *, mean(some_value) OVER (
  *    PARTITION BY id
  *    ORDER BY CAST(start AS timestamp)
  *    RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
  * ) AS mean
  * FROM myTable
  * }}}
  * @see [[https://stackoverflow.com/questions/33207164/spark-window-functions-rangebetween-dates]]
  */
case class Over(expression: Expression,
                partitionBy: Seq[Field] = Nil,
                orderBy: Seq[OrderColumn] = Nil,
                range: Option[Condition] = None) extends Expression
