package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Split(string, delimiter) function
  * @param string    the given string
  * @param delimiter the delimiter to use for splitting the string
  */
case class Split(string: Expression, delimiter: Expression) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Any] = {
    for {
      str <- string.getAsString(scope)
      delim <- delimiter.getAsString(scope)
    } yield str.split(s"[$delim]")
  }
}
