package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Width(string, length) function
  * @param string the given string
  * @param length the length of the substring
  */
case class Width(string: Expression, length: Expression) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Any] = {
    length.getAsInt(scope).map(sizeTo(string.getAsString(scope), _))
  }

  private def sizeTo(value: Option[String], width: Int): String = {
    value match {
      case Some(s) if s.length == width => s
      case Some(s) if s.length > width => s.take(width)
      case Some(s) if s.length < width => s + (" " * (width - s.length))
      case None => " " * width
    }
  }
}
