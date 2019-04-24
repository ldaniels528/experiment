package com.qwery.models.expressions

import com.qwery.models.expressions.IntervalTypes.IntervalType

/**
  * Represents an Interval (e.g. "7 YEARS")
  * @param count  the [[Expression numeric]] interval
  * @param `type` the [[IntervalType interval type]]
  */
case class Interval(count: Expression, `type`: IntervalType) extends Expression

/**
  * Represents an Interval Type (e.g. "7 YEARS")
  * @author lawrence.daniels@gmail.com
  */
object IntervalTypes extends Enumeration {
  type IntervalType = Value

  val MICROSECOND, SECOND, MINUTE, DAY, WEEK, MONTH, QUARTER, YEAR,
  SECOND_MICROSECOND, MINUTE_MICROSECOND, MINUTE_SECOND, HOUR_MICROSECOND, HOUR_SECOND,
  HOUR_MINUTE, DAY_MICROSECOND, DAY_SECOND, DAY_MINUTE, DAY_HOUR, YEAR_MONTH: IntervalType = Value

  val intervalTypes: Map[String, IntervalType] = Map(
    "MICROSECOND" -> MICROSECOND,
    "SECOND" -> SECOND,
    "MINUTE" -> MINUTE,
    "DAY" -> DAY,
    "WEEK" -> WEEK,
    "MONTH" -> MONTH,
    "QUARTER" -> QUARTER,
    "YEAR" -> YEAR,
    "SECOND_MICROSECOND" -> SECOND_MICROSECOND,
    "MINUTE_MICROSECOND" -> MINUTE_MICROSECOND,
    "MINUTE_SECOND" -> MINUTE_SECOND,
    "HOUR_MICROSECOND" -> HOUR_MICROSECOND,
    "HOUR_SECOND" -> HOUR_SECOND,
    "HOUR_MINUTE" -> HOUR_MINUTE,
    "DAY_MICROSECOND" -> DAY_MICROSECOND,
    "DAY_SECOND" -> DAY_SECOND,
    "DAY_MINUTE" -> DAY_MINUTE,
    "DAY_HOUR" -> DAY_HOUR,
    "YEAR_MONTH" -> YEAR_MONTH
  )
}

