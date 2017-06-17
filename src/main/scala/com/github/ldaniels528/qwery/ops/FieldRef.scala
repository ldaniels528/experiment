package com.github.ldaniels528.qwery.ops

/**
  * Represents a field reference
  * @param name the name of the field being referenced
  */
case class FieldRef(name: String) extends NamedExpression {

  override def evaluate(scope: Scope): Option[Any] = scope.get(name)

}
