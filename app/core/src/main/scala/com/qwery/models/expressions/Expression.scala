package com.qwery.models
package expressions

/**
  * Represents an expression; which in its simplest form is a value (boolean, double or string)
  * @author lawrence.daniels@gmail.com
  */
trait Expression extends Aliasable

/**
 * Represents an array
 * @param values the values within the array
 */
case class ArrayExpression(values: Any*) extends Expression

/**
  * Casts the value `expr` to the target data type type.
  * @example cast(expr AS type)
  */
case class Cast(value: Expression, toType: ColumnSpec) extends Expression

/**
  * Current Row
  * @see [[Over]]
  */
case object CurrentRow extends Expression

/**
  * If expr1 evaluates to true, then returns expr2; otherwise returns expr3.
  * @example if(expr1, expr2, expr3)
  */
case class If(condition: Condition, trueValue: Expression, falseValue: Expression) extends Expression

/**
  * Represents a literal value (e.g. "Hello")
  * @param value the given value
  */
case class Literal(value: Any) extends Expression

/**
 * Represents an hash map
 * @param values the values within the hash map
 */
case class MapExpression(values: Map[String, Any]) extends Expression

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
