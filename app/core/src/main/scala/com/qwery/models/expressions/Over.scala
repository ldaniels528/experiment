package com.qwery.models.expressions

import com.qwery.models.OrderColumn
import com.qwery.models.expressions.Over.BetweenTypes.BetweenType
import com.qwery.models.expressions.Over.RangeOrRowsBetween

/**
  * Window-Over clause
  * @param expression  the host [[Expression]]
  * @param partitionBy the given partition by columns
  * @param orderBy     the given order by columns
  * @param between     the [[RangeOrRowsBetween RANGE/ROWS BETWEEN]] clause
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
                between: Option[RangeOrRowsBetween] = None) extends Expression

/**
  * Over Companion
  * @author lawrence.daniels@gmail.com
  */
object Over {

  /**
    * Between Types
    * @author lawrence.daniels@gmail.com
    */
  object BetweenTypes extends Enumeration {
    type BetweenType = Value
    val ROWS, RANGE: BetweenType = Value
  }

  case class Following(expr: Expression) extends Expression

  case class Preceding(expr: Expression) extends Expression

  /**
    * SQL: RANGE/ROWS BETWEEN `expression` [PRECEDING] AND `expression`
    * @param betweenType the given [[BetweenType between type]]
    * @param from        the lower bound [[Expression expression]]
    * @param to          the upper bound [[Expression expression]]
    * @example {{{ RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW }}}
    */
  case class RangeOrRowsBetween(betweenType: BetweenType, from: Expression, to: Expression) extends Condition

}
