package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Right(string, length) function
  * @param string the given string
  * @param length the length of the substring
  */
case class Right(string: Expression, length: Expression) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Any] = {
    for {
      str <- string.getAsString(scope)
      len <- length.getAsInt(scope) if str.length >= len
    } yield str.substring(str.length - len, str.length)
  }
}
