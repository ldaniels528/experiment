package com.qwery.models.expressions

/**
  * Represents a SQL function
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction extends Field {
  val name: String = getClass.getSimpleName.toLowerCase().replaceAllLiterally("$", "")

  override def toString: String = name
}

sealed trait SQLFunction1 extends SQLFunction {

  def field: Expression

  override val toString = s"$name($field)"
}

sealed trait SQLFunction2 extends SQLFunction {

  def field1: Expression

  def field2: Expression

  override val toString = s"$name($field1,$field2)"
}

sealed trait SQLFunction3 extends SQLFunction {

  def field1: Expression

  def field2: Expression

  def field3: Expression

  override val toString = s"$name($field1,$field2,$field3)"
}

case class Avg(field: Expression) extends SQLFunction1 {
  override val isAggregate = true
}

case class Cast(field1: Expression, field2: Expression) extends SQLFunction2

case class Concat(field1: Expression, field2: Expression) extends SQLFunction2

case class Count(field: Expression) extends SQLFunction1 {
  override val isAggregate: Boolean = true
}

case class Max(field: Expression) extends SQLFunction1 {
  override val isAggregate = true
}

case class Min(field: Expression) extends SQLFunction1 {
  override val isAggregate = true
}

case class PadLeft(field1: Expression, field2: Expression) extends SQLFunction2

case class PadRight(field1: Expression, field2: Expression) extends SQLFunction2

case class Substring(field1: Expression, field2: Expression, field3: Expression) extends SQLFunction3

case class Sum(field: Expression) extends SQLFunction1 {
  override def isAggregate: Boolean = true
}

case class StdDev(field: Expression) extends SQLFunction1 {
  override val isAggregate = true
}