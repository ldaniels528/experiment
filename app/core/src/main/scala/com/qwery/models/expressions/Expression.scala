package com.qwery.models.expressions

import com.qwery.models.Aliasable

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
    Seq("Boolean", "Byte", "Date", "Double", "Float", "Int", "Integer", "Long", "Short", "String", "Timestamp", "UUID")

  def isValidType(typeName: String): Boolean = validTypes.exists(_ equalsIgnoreCase typeName)

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
  * Represents a Null value
  */
case object Null extends Expression
