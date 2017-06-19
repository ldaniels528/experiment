package com.github.ldaniels528.qwery.ops

/**
  * Represents a column reference
  * @param name the name of the column being referenced
  */
case class ColumnRef(name: String) extends NamedExpression {

  override def evaluate(scope: Scope): Option[Any] = scope.get(name)

}

/**
  * Represents a join column reference
  * @param alias the data resource alias
  * @param name the name of the column being referenced
  */
case class JoinColumnRef(alias: String, name: String) extends NamedExpression {

  override def evaluate(scope: Scope): Option[Any] = scope.row(alias).flatMap(_.get(name))

}