package com.github.ldaniels528.qwery.ops

/**
  * Represents a reference to a variable
  * @author lawrence.daniels@gmail.com
  */
case class VariableRef(name: String) extends NamedExpression {

  override def evaluate(scope: Scope): Option[Any] = {
    val variable = scope.lookupVariable(name)
      .getOrElse(throw new IllegalArgumentException(s"No such variable '$name'"))
    variable.evaluate(scope)
  }

  override def toString = s"@$name"

}
