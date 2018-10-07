package com.qwery.models.expressions

import com.qwery.models.ColumnTypes.ColumnType

/**
  * Represents a SQL function
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction extends NamedExpression {
  val name: String = getClass.getSimpleName.toLowerCase().replaceAllLiterally("$", "")
  override val toString: String = s"$name()"
}

/**
  * Represents a SQL function with a single parameter
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction1 extends SQLFunction {
  override val toString = s"$name($field)"

  def field: Expression
}

/**
  * Represents a SQL function with two parameters
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction2 extends SQLFunction {
  override val toString = s"$name($field1,$field2)"

  def field1: Expression

  def field2: Expression
}

/**
  * Represents a SQL function with three parameters
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction3 extends SQLFunction {
  override val toString = s"$name($field1,$field2,$field3)"

  def field1: Expression

  def field2: Expression

  def field3: Expression
}

/**
  * Represents a SQL function with any number of parameters
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunctionN extends SQLFunction {
  override val toString = s"$name(${args mkString ","})"

  def args: List[Expression]
}

/**
  * Represents an aggregation function
  * @author lawrence.daniels@gmail.com
  */
sealed trait Aggregation extends NamedExpression {
  override val isAggregate = true
}

/////////////////////////////////////////////////////////////////////
//    SQL Function Library
/////////////////////////////////////////////////////////////////////

case class Abs(field: Expression) extends SQLFunction1

case class Add_Months(field1: Expression, field2: Expression) extends SQLFunction2

case class Array_Contains(field1: Expression, field2: Expression) extends SQLFunction2

case class Ascii(field: Expression) extends SQLFunction1

case class Avg(field: Expression) extends SQLFunction1 with Aggregation

case class Base64(field: Expression) extends SQLFunction1

case class Bin(field: Expression) extends SQLFunction1

case class Cast(value: Expression, toType: ColumnType) extends SQLFunction

case class Cbrt(field: Expression) extends SQLFunction1

case class Ceil(field: Expression) extends SQLFunction1

case class Coalesce(args: List[Expression]) extends SQLFunctionN

case class Concat(field1: Expression, field2: Expression) extends SQLFunction2

case class Count(field: Expression) extends SQLFunction1 with Aggregation

case object Cume_Dist extends SQLFunction

case object Current_Date extends SQLFunction

case class Date_Add(field1: Expression, field2: Expression) extends SQLFunction2

case class Distinct(field: Expression) extends SQLFunction1 with Aggregation

case class Factorial(field: Expression) extends SQLFunction1

case class Floor(field: Expression) extends SQLFunction1

case class If(condition: Condition, trueValue: Expression, falseValue: Expression) extends SQLFunction

case class Lower(field: Expression) extends SQLFunction1

case class LPad(field1: Expression, field2: Expression, field3: Expression) extends SQLFunction3

case class Max(field: Expression) extends SQLFunction1 with Aggregation

case class Min(field: Expression) extends SQLFunction1 with Aggregation

case class RPad(field1: Expression, field2: Expression, field3: Expression) extends SQLFunction3

case class Substring(field1: Expression, field2: Expression, field3: Expression) extends SQLFunction3

case class Sum(field: Expression) extends SQLFunction1 with Aggregation

case class StdDev(field: Expression) extends SQLFunction1 with Aggregation

case class To_Date(field: Expression) extends SQLFunction1

case class Trim(field: Expression) extends SQLFunction1

case class Upper(field: Expression) extends SQLFunction1

case class Variance(field: Expression) extends SQLFunction1 with Aggregation

case class WeekOfYear(field: Expression) extends SQLFunction1

case class Year(field: Expression) extends SQLFunction1