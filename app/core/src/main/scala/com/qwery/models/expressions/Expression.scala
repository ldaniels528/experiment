package com.qwery.models.expressions

import java.util.concurrent.atomic.AtomicInteger

/**
  * Represents an expression; while in its simplest form is a value (boolean, double or string)
  * @author lawrence.daniels@gmail.com
  */
trait Expression

case class Add(a: Expression, b: Expression) extends Expression

/**
  * Represents a CASE expression
  * @example
  * {{{
  * CASE primary-expr
  *   WHEN expr1 THEN result1
  *   WHEN expr2 THEN result2
  *   ELSE expr3
  * END
  * }}}
  * @example
  * {{{
  * CASE
  *   WHEN value1 = expr1 THEN result1
  *   WHEN value2 = expr2 THEN result2
  *   ELSE expr3
  * END
  * }}}
  * @param conditions the list of WHEN conditions
  */
case class Case(conditions: List[When], otherwise: Option[Expression]) extends Expression

object Case {
  def apply(conditions: When*)(otherwise: Option[Expression]): Case = new Case(conditions.toList, otherwise)
}

case class Divide(a: Expression, b: Expression) extends Expression

/**
  * Represents a literal value (e.g. "Hello")
  * @param value the literal value
  */
case class Literal(value: Any) extends Expression

case class Modulo(a: Expression, b: Expression) extends Expression

case class Multiply(a: Expression, b: Expression) extends Expression

case object Null extends Expression

case class Pow(a: Expression, b: Expression) extends Expression

case class Subtract(a: Expression, b: Expression) extends Expression

/**
  * Represents a WHEN condition
  * @param condition the given [[Condition condition]]
  * @param result    the given [[Expression result expression]]
  */
case class When(condition: Condition, result: Expression)

/**
  * Expression Companion
  * @author lawrence.daniels@gmail.com
  */
object Expression {
  val validTypes = Seq("Boolean", "Byte", "Date", "Double", "Float", "Int", "Integer", "Long", "Short", "String", "UUID")
  var ticker = new AtomicInteger()

  def isValidType(typeName: String): Boolean = validTypes.exists(_.equalsIgnoreCase(typeName))

  /**
    * Expression Implicits
    * @author lawrence.daniels@gmail.com
    */
  object Implicits {

    /**
      * Expression Extensions
      * @param expr0 the given [[Expression value]]
      */
    final implicit class ExpressionExtensions(val expr0: Expression) extends AnyVal {

      @inline def ===(expr1: Expression) = EQ(expr0, expr1)

      @inline def !==(expr1: Expression) = NE(expr0, expr1)

      @inline def >(expr1: Expression) = GT(expr0, expr1)

      @inline def >=(expr1: Expression) = GE(expr0, expr1)

      @inline def <(expr1: Expression) = LT(expr0, expr1)

      @inline def <=(expr1: Expression) = LE(expr0, expr1)

      @inline def +(expr1: Expression) = Add(expr0, expr1)

      @inline def ||(expr1: Expression) = Concat(expr0, expr1)

      @inline def -(expr1: Expression) = Subtract(expr0, expr1)

      @inline def *(expr1: Expression) = Multiply(expr0, expr1)

      @inline def **(expr1: Expression) = Pow(expr0, expr1)

      @inline def /(expr1: Expression) = Divide(expr0, expr1)

    }

    final implicit def string2Expr(value: String): Expression = Literal(value)

    final implicit def int2Expr(value: Int): Expression = Literal(value)

    final implicit def double2Expr(value: Double): Expression = Literal(value)

    final implicit def long2Expr(value: Long): Expression = Literal(value)

    /**
      * Condition Extensions
      * @param cond0 the given [[Condition condition]]
      */
    final implicit class ConditionExtensions(val cond0: Condition) extends AnyVal {

      @inline def ! = NOT(cond0)

      @inline def &&(cond1: Condition) = AND(cond0, cond1)

      @inline def ||(cond1: Condition) = OR(cond0, cond1)

    }

  }

}
