package com.github.ldaniels528.qwery.ops

/**
  * math package object
  * @author lawrence.daniels@gmail.com
  */
package object math {

  /**
    * Expression Extensions
    * @param value the given [[Expression value]]
    */
  final implicit class ExpressionExtensions(val value: Expression) extends AnyVal {

    def +(that: Expression) = Add(value, that)

    def -(that: Expression) = Subtract(value, that)

    def *(that: Expression) = Multiply(value, that)

    def /(that: Expression) = Divide(value, that)

    def |(that: Expression) = Concat(value, that)

  }

}
