package com.qwery.models.expressions

import com.qwery.models.OrderColumn
import com.qwery.models.expressions.Over.RangeBetween

/**
  * Window-Over clause
  * @param expression  the host [[Expression]]
  * @param partitionBy the given partition by columns
  * @param orderBy     the given order by columns
  * @param range       the [[RangeBetween RANGE BETWEEN]] clause
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
                range: Option[RangeBetween] = None) extends Expression

/**
  * Over Companion
  * @author lawrence.daniels@gmail.com
  */
object Over {

  /**
    * SQL: RANGE BETWEEN `expression` AND `expression`
    * @param from the lower bound [[Expression expression]]
    * @param to   the upper bound [[Expression expression]]
    * @example {{{ RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW }}}
    */
  case class RangeBetween(from: Expression, to: Expression) extends Condition

}
