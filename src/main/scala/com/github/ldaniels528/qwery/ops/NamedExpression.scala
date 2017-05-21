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
  private val random = new Random()
  private val namingCharSet: List[Char] = ('A' to 'Z').toList ::: ('a' to 'z').toList

  /**
    * Returns a named alias for the given expression
    * @param name       the name of the alias
    * @param expression the expression
    * @return a named alias
    */
  def apply(name: String, expression: Expression): NamedExpression = expression match {
    case aggregate: Expression with Aggregation => NamedAggregation(name, aggregate)
    case vanilla => NamedAlias(name, vanilla)
  }

  /**
    * For pattern matching
    */
  def unapply(expression: NamedExpression): Option[String] = Some(expression.name)

  /**
    * Represents a named alias for an aggregate field or expression
    * @author lawrence.daniels@gmail.com
    */
  case class NamedAggregation(name: String, aggregate: Expression with Aggregation) extends NamedExpression with Aggregation {

    override def evaluate(scope: Scope): Option[Any] = aggregate.evaluate(scope)

    override def update(scope: Scope): Unit = aggregate.update(scope)

  }

  /**
    * Represents a named alias for a field or expression
    * @author lawrence.daniels@gmail.com
    */
  case class NamedAlias(name: String, expression: Expression) extends NamedExpression {

    override def evaluate(scope: Scope): Option[Any] = expression.evaluate(scope)

  }

  /**
    * Expression Extensions
    * @param expression the given [[Expression expression]]
    */
  final implicit class ExpressionExtensions(val expression: Expression) extends AnyVal {

    def getName: String = expression match {
      case NamedExpression(name) => name
      case _ => randomName()
    }
  }

  def randomName(length: Int = 8): String = {
    (1 to length).map(_ => namingCharSet(random.nextInt(namingCharSet.length))).mkString
  }

}
