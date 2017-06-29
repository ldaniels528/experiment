package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.Expression

/**
  * Represents an internal function
  * @author lawrence.daniels@gmail.com
  */
trait InternalFunction extends Expression {
  val name: String = getClass.getSimpleName.toUpperCase().replaceAllLiterally("$", "")
}
