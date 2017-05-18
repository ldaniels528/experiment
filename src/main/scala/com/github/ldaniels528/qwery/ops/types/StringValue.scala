package com.github.ldaniels528.qwery.ops.types

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Represents a string value
  * @author lawrence.daniels@gmail.com
  */
case class StringValue(value: String) extends Expression {

  override def compare(that: Expression, scope: Scope): Int = {
    that.evaluate(scope).map {
      case s: String => value.compareTo(s)
      case _ => -1
    } getOrElse -1
  }

  override def evaluate(scope: Scope): Option[String] = Option(value)
}