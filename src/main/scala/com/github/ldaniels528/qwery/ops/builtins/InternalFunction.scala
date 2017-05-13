package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.Expression

/**
  * Represents an internal function
  * @author lawrence.daniels@gmail.com
  */
trait InternalFunction extends Expression {
  val name: String = getClass.getSimpleName.toUpperCase().replaceAllLiterally("$", "")
}

/**
  * Represents a parameter-less internal function
  * @author lawrence.daniels@gmail.com
  */
trait InternalFunction0 extends InternalFunction {
  override def toSQL: String = s"$name()"
}

/**
  * Represents a single-parameter internal function
  * @author lawrence.daniels@gmail.com
  */
trait InternalFunction1 extends InternalFunction {
  def expression: Expression

  override def toSQL: String = s"$name(${expression.toSQL})"
}
