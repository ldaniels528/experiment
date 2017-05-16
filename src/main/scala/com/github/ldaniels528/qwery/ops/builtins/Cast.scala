package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * CAST(expression AS type) function
  * @param expression the expression for which is the input data
  * @param toType     the express which describes desired type
  */
case class Cast(expression: Expression, toType: String) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Any] = {
    toType match {
      case s if s.equalsIgnoreCase("Boolean") => expression.getAsBoolean(scope)
      case s if s.equalsIgnoreCase("Byte") => expression.getAsByte(scope)
      case s if s.equalsIgnoreCase("Double") => expression.getAsDouble(scope)
      case s if s.equalsIgnoreCase("Float") => expression.getAsFloat(scope)
      case s if s.equalsIgnoreCase("Int") => expression.getAsInt(scope)
      case s if s.equalsIgnoreCase("Long") => expression.getAsLong(scope)
      case s if s.equalsIgnoreCase("Short") => expression.getAsShort(scope)
      case s if s.equalsIgnoreCase("String") => expression.getAsString(scope)
      case theType =>
        throw new IllegalStateException(s"Invalid conversion type $theType")
    }
  }
}