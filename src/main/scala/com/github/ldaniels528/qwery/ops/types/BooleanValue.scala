package com.github.ldaniels528.qwery.ops.types

import com.github.ldaniels528.qwery.ops.types.BooleanValue._
import com.github.ldaniels528.qwery.ops.{Expression, Field, Scope}

/**
  * Represents a boolean value
  * @author lawrence.daniels@gmail.com
  */
case class BooleanValue(value: Boolean) extends Expression {

  override def compare(that: Expression, scope: Scope): Int = {
    that match {
      case NumericValue(v) => value.compareTo(java.lang.Boolean.valueOf(v == 0))
      case StringValue(v) => value.compareTo(booleans.exists(_.equalsIgnoreCase(v)))
      case field: Field => field.compare(this, scope)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

  override def evaluate(scope: Scope): Option[Boolean] = Option(value)

}

/**
  * Boolean Value
  * @author lawrence.daniels@gmail.com
  */
object BooleanValue {
  private val booleans = Seq("on", "true", "yes")

}