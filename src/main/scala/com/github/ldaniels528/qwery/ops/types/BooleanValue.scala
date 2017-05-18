package com.github.ldaniels528.qwery.ops.types

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Represents a boolean value
  * @author lawrence.daniels@gmail.com
  */
case class BooleanValue(value: Boolean) extends Expression {

  override def compare(that: Expression, scope: Scope): Int = {
    that.evaluate(scope).map {
      case b: Boolean => value.compareTo(b)
      case _ => -1
    } getOrElse -1
  }

  override def evaluate(scope: Scope): Option[Boolean] = Option(value)

}
