package com.github.ldaniels528.qwery.ops

/**
  * Represents an Aggregate Entity
  * @author lawrence.daniels@gmail.com
  */
trait Aggregation {

  def update(scope: Scope): Unit

}

/**
  * Represents an Aggregate Expression
  * @author lawrence.daniels@gmail.com
  */
case class AggregateExpression(name: String, expression: Expression) extends NamedExpression with Aggregation {
  private var value: Option[Any] = None

  override def evaluate(scope: Scope): Option[Any] = value

  override def update(scope: Scope): Unit = value = expression.evaluate(scope)
}
