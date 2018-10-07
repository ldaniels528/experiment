package com.qwery.models.expressions

import java.util.concurrent.atomic.AtomicInteger

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
  val validTypes = Seq("Boolean", "Byte", "Date", "Double", "Float", "Int", "Integer", "Long", "Short", "String", "UUID")
  var ticker = new AtomicInteger()

  def isValidType(typeName: String): Boolean = validTypes.exists(_ equalsIgnoreCase typeName)

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

    /**
      * String to Expression conversion
      * @param value the given String value
      * @return the equivalent [[Literal]]
      */
    final implicit def string2Expr(value: String): Literal = Literal(value)

    /**
      * Integer to Expression conversion
      * @param value the given Integer value
      * @return the equivalent [[Literal]]
      */
    final implicit def int2Expr(value: Int): Literal = Literal(value)

    /**
      * Double to Expression conversion
      * @param value the given Double value
      * @return the equivalent [[Literal]]
      */
    final implicit def double2Expr(value: Double): Literal = Literal(value)

    /**
      * Long to Expression conversion
      * @param value the given Long value
      * @return the equivalent [[Literal]]
      */
    final implicit def long2Expr(value: Long): Literal = Literal(value)

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
