package com.github.ldaniels528.qwery.ops

/**
  * types package object
  * @author lawrence.daniels@gmail.com
  */
package object types {

  implicit def booleanConversion(value: Boolean): Expression = BooleanValue(value)

  implicit def doubleConversion(value: Double): Expression = NumericValue(value)

  implicit def intConversion(value: Int): Expression = NumericValue(value)

  implicit def longConversion(value: Long): Expression = NumericValue(value)

  implicit def numberConversion(value: Number): Expression = NumericValue(value.doubleValue())

  implicit def stringConversion(value: String): Expression = StringValue(value)

}
