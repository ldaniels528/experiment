package com.qwery.models

import com.qwery.models.Over.Range
import com.qwery.models.expressions.Field

/**
  * Window-Over clause
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
class Over(partitionBy: Seq[Field] = Nil,
           orderBy: Seq[OrderColumn] = Nil,
           range: Option[Range] = None) extends Invokable

/**
  * Over Companion
  * @author lawrence.daniels@gmail.com
  */
object Over {

  sealed trait Range

  class RangeBetween() extends Range

}