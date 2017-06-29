package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Substring(str, start, length) function
  * @param string   the given string
  * @param startPos the starting position of the substring
  * @param length   the length of the substring
  */
case class Substring(string: Expression, startPos: Expression, length: Expression) extends InternalFunction {

  override def evaluate(scope: Scope): Option[Any] = {
    for {
      str <- string.getAsString(scope)
      start <- startPos.getAsInt(scope) if start < str.length
      len <- length.getAsInt(scope)
    } yield str.substring(start, start + len)
  }

}
