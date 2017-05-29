package com.github.ldaniels528.qwery.ops

/**
  * Represents a variable
  * @author lawrence.daniels@gmail.com
  */
case class Variable(name: String, var value: Option[Any] = None) extends NamedExpression {

  override def evaluate(scope: Scope): Option[Any] = value

  def set(scope: Scope, expression: Expression): Option[Any] = {
    this.value = expression.evaluate(scope)
    this.value
  }

}
