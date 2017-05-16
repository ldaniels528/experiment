package com.github.ldaniels528.qwery.ops

/**
  * Represents a reference to a function
  * @author lawrence.daniels@gmail.com
  */
case class FunctionRef(name: String, args: Seq[Expression]) extends NamedExpression {

  override def evaluate(scope: Scope): Option[Any] = {
    scope.lookup(this).flatMap(_.invoke(scope, args))
  }

}