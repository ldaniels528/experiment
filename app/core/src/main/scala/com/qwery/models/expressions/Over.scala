package com.qwery.models.expressions

import com.qwery.models.OrderColumn
import com.qwery.models.expressions.Over.DataAccessTypes.DataAccessType

/**
  * Window-Over clause
  * @param expression  the host [[Expression]]
  * @param partitionBy the given partition by columns
  * @param orderBy     the given order by columns
  * @param modifier    the given [[Expression access modifier]]
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
                partitionBy: Seq[FieldRef] = Nil,
                orderBy: Seq[OrderColumn] = Nil,
                modifier: Option[Expression] = None) extends Expression

/**
  * Over Companion
  * @author lawrence.daniels@gmail.com
  */
object Over {

  /**
    * Data Access Types
    * @author lawrence.daniels@gmail.com
    */
  object DataAccessTypes extends Enumeration {
    type DataAccessType = Value
    val ROWS, RANGE: DataAccessType = Value
  }

  /**
    * SQL: RANGE BETWEEN INTERVAL 7 DAYS FOLLOWING AND CURRENT ROW
    * @param expr the given [[Expression]]
    */
  case class Following(expr: Expression) extends Expression

  /**
    * SQL: ROWS BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
    * @param expr the given [[Expression]]
    */
  case class Preceding(expr: Expression) extends Expression

  /**
    * SQL: RANGE/ROWS UNBOUNDED [FOLLOWING/PRECEDING]
    * @param accessType the given [[DataAccessType data access type]]
    */
  case class Unbounded(accessType: DataAccessType) extends Expression

  /**
    * SQL: RANGE/ROWS BETWEEN `expression` [PRECEDING] AND `expression`
    * @param accessType the given [[DataAccessType data access type]]
    * @param from       the lower bound [[Expression expression]]
    * @param to         the upper bound [[Expression expression]]
    * @example {{{ RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW }}}
    */
  case class WindowBetween(accessType: DataAccessType, from: Expression, to: Expression) extends Condition

}
