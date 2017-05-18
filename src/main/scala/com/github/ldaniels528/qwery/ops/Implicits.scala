package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.builtins._
import com.github.ldaniels528.qwery.ops.types.{BooleanValue, DateValue, NumericValue, StringValue}

/**
  * Ops Implicits
  * @author lawrence.daniels@gmail.com
  */
object Implicits {

  implicit def booleanConversion(value: Boolean): BooleanValue = BooleanValue(value)

  implicit def dateConversion(value: java.util.Date): DateValue = DateValue(value)

  implicit def doubleConversion(value: Double): NumericValue = NumericValue(value)

  implicit def intConversion(value: Int): NumericValue = NumericValue(value)

  implicit def longConversion(value: Long): NumericValue = NumericValue(value)

  implicit def numberConversion(value: Number): NumericValue = NumericValue(value.doubleValue())

  implicit def stringConversion(value: String): StringValue = StringValue(value)

  /**
    * Expression Extensions
    * @param expr0 the given [[Expression value]]
    */
  final implicit class ExpressionExtensions(val expr0: Expression) extends AnyVal {

    def ===(expr1: Expression) = EQ(expr0, expr1)

    def !==(expr1: Expression) = NE(expr0, expr1)

    def +(expr1: Expression) = Add(expr0, expr1)

    def -(expr1: Expression) = Subtract(expr0, expr1)

    def *(expr1: Expression) = Multiply(expr0, expr1)

    def **(expr1: Expression) = Pow(expr0, expr1)

    def /(expr1: Expression) = Divide(expr0, expr1)

  }

  /**
    * Condition Extensions
    * @param cond0 the given [[Condition condition]]
    */
  final implicit class ConditionExtensions(val cond0: Condition) extends AnyVal {

    def ! = NOT(cond0)

    def &&(cond1: Condition) = AND(cond0, cond1)

    def ||(cond1: Condition) = OR(cond0, cond1)

  }

}
