package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.util.OptionHelper._
import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * PadLeft(string, width) function
  * @param string the given string
  * @param width the width of the substring
  */
case class PadLeft(string: Expression, width: Expression) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Any] = {
    val result = for {
      w <- width.getAsInt(scope)
      s <- string.getAsString(scope)
    } yield padLeft(s, w)

    result ?? width.getAsInt(scope).map(" " * _)
  }

  private def padLeft(value: String, width: Int): String = {
    value match {
      case s if s.length == width => s
      case s if s.length > width => s.take(width)
      case s if s.length < width => (" " * (width - s.length)) + s
    }
  }
}
