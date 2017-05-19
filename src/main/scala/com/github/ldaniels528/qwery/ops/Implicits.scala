package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.builtins._

/**
  * Ops Implicits
  * @author lawrence.daniels@gmail.com
  */
object Implicits {

  implicit def booleanConversion(value: Boolean): ConstantValue = ConstantValue(value)

  implicit def dateConversion(value: java.util.Date): ConstantValue = ConstantValue(value)

  implicit def doubleConversion(value: Double): ConstantValue = ConstantValue(value)

  implicit def floatConversion(value: Float): ConstantValue = ConstantValue(value)

  implicit def intConversion(value: Int): ConstantValue = ConstantValue(value)

  implicit def longConversion(value: Long): ConstantValue = ConstantValue(value)

  implicit def numberConversion(value: Number): ConstantValue = ConstantValue(value.doubleValue())

  implicit def shortConversion(value: Short): ConstantValue = ConstantValue(value)

  implicit def stringConversion(value: String): ConstantValue = ConstantValue(value)

  /**
    * Expression Extensions
    * @param expr0 the given [[Expression value]]
    */
  final implicit class ExpressionExtensions(val expr0: Expression) extends AnyVal {

    def ===(expr1: Expression) = EQ(expr0, expr1)

    def !==(expr1: Expression) = NE(expr0, expr1)

    def +(expr1: Expression) = Add(expr0, expr1)

    def ||(expr1: Expression) = Concat(expr0, expr1)

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
