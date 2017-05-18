package com.github.ldaniels528.qwery.ops.types

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Represents a numeric value
  * @author lawrence.daniels@gmail.com
  */
case class NumericValue(value: Double) extends Expression {

  override def compare(that: Expression, scope: Scope): Int = {
    that.evaluate(scope).map(_.asInstanceOf[AnyRef]).map {
      case n: Number => value.compareTo(n.doubleValue())
      case _ => -1
    } getOrElse -1
  }

  override def evaluate(scope: Scope): Option[Double] = Option(value)

}
