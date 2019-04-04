package com.qwery.models.expressions

import com.qwery.models.Aliasable

/**
  * Represents an expression; which in its simplest form is a value (boolean, double or string)
  * @author lawrence.daniels@gmail.com
  */
trait Expression extends Aliasable

/**
  * Add expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class Add(a: Expression, b: Expression) extends Expression

/**
  * Bitwise AND expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class BitwiseAND(a: Expression, b: Expression) extends Expression

/**
  * Bitwise OR expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class BitwiseOR(a: Expression, b: Expression) extends Expression

/**
  * Bitwise Exclusive OR expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class BitwiseXOR(a: Expression, b: Expression) extends Expression

/**
  * Divide expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class Divide(a: Expression, b: Expression) extends Expression

/**
  * Represents a literal value (e.g. "Hello")
  * @param value the given value
  */
case class Literal(value: Any) extends Expression

/**
  * Modulo expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class Modulo(a: Expression, b: Expression) extends Expression

/**
  * Multiply expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class Multiply(a: Expression, b: Expression) extends Expression

/**
  * Represents a Null value
  */
case object Null extends Expression

/**
  * Power/exponent expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class Pow(a: Expression, b: Expression) extends Expression

/**
  * Subtract expression
  * @param a the left-side [[Expression]]
  * @param b the right-side [[Expression]]
  */
case class Subtract(a: Expression, b: Expression) extends Expression

/**
  * Expression Companion
  * @author lawrence.daniels@gmail.com
  */
object Expression {
  val validTypes: Seq[String] =
    Seq("Boolean", "Byte", "Date", "Double", "Float", "Int", "Integer", "Long", "Short", "String", "Timestamp", "UUID")

  def isValidType(typeName: String): Boolean = validTypes.exists(_ equalsIgnoreCase typeName)

}
