package com.github.ldaniels528.qwery.ops

import scala.util.Random

/**
  * Represents a Named Expression
  * @author lawrence.daniels@gmail.com
  */
trait NamedExpression extends Expression {

  def name: String

}

/**
  * Named Expression
  * @author lawrence.daniels@gmail.com
  */
object NamedExpression {

  /**
    * Returns a named alias for the given expression
    * @param name       the name of the alias
    * @param expression the expression
    * @return a named alias
    */
  def apply(name: String, expression: Expression): NamedExpression = expression match {
    case aggregate: Expression with Aggregation => AggregateAlias(name, aggregate)
    case vanilla => ExpressionAlias(name, vanilla)
  }

  /**
    * For pattern matching
    */
  def unapply(expression: NamedExpression): Option[String] = Some(expression.name)

  /**
    * Represents an alias for a field or expression
    * @author lawrence.daniels@gmail.com
    */
  case class AggregateAlias(name: String, aggregate: Expression with Aggregation) extends NamedExpression with Aggregation {

    override def evaluate(scope: Scope): Option[Any] = aggregate.evaluate(scope)

    override def update(scope: Scope): Unit = aggregate.update(scope)

  }

  /**
    * Represents an alias for a field or expression
    * @author lawrence.daniels@gmail.com
    */
  case class ExpressionAlias(name: String, expression: Expression) extends NamedExpression {

    override def evaluate(scope: Scope): Option[Any] = expression.evaluate(scope)

  }

  /**
    * Expression Extensions
    * @param expression the given [[Expression expression]]
    */
  final implicit class ExpressionExtensions(val expression: Expression) extends AnyVal {

    def getName: String = expression match {
      case NamedExpression(name) => name
      case _ => randomName
    }
  }

  private def randomName: String = {
    val random = new Random()
    val chars = for (_ <- 1 to 16) yield (random.nextInt('z' - 'A') + 'A').toChar
    String.copyValueOf(chars.filter(_.isLetterOrDigit).toArray).take(8)
  }

}
