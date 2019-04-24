package com.qwery.models.expressions

import com.qwery.models.Aliasable
import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models.expressions.IntervalTypes.IntervalType

/**
  * Represents an expression; which in its simplest form is a value (boolean, double or string)
  * @author lawrence.daniels@gmail.com
  */
trait Expression extends Aliasable

/**
  * Expression Companion
  * @author lawrence.daniels@gmail.com
  */
object Expression {
  val validTypes: Seq[String] =
    Seq("Boolean", "Byte", "Char", "Date", "Double", "Float", "Int", "Integer", "Long", "Short", "String", "Timestamp", "UUID")

  def isValidType(typeName: String): Boolean = validTypes.exists(_ equalsIgnoreCase typeName)

}

/**
  * Casts the value expr to the target data type type.
  * @example cast(expr AS type)
  */
case class Cast(value: Expression, toType: ColumnType) extends Expression

/**
  * If expr1 evaluates to true, then returns expr2; otherwise returns expr3.
  * @example if(expr1, expr2, expr3)
  */
case class If(condition: Condition, trueValue: Expression, falseValue: Expression) extends Expression

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

/**
  * Represents a literal value (e.g. "Hello")
  * @param value the given value
  */
case class Literal(value: Any) extends Expression

/**
  * Represents a mathematical expression
  * @param expr0    the left-side [[Expression]]
  * @param expr1    the right-side [[Expression]]
  * @param operator the math operator (e.g. "+")
  */
case class MathOp(expr0: Expression, expr1: Expression, operator: String) extends Expression

/**
  * Represents a Null value or expression
  */
case object Null extends Expression
