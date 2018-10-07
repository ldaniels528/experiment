package com.qwery.models.expressions

import java.util.concurrent.atomic.AtomicInteger
import com.qwery.util.OptionHelper._
import com.qwery.models.Aliasable

/**
  * Represents a Named Expression
  * @author lawrence.daniels@gmail.com
  */
trait NamedExpression extends Expression {

  /**
    * @return the name of the expression
    */
  def name: String

  /**
    * @return indicates whether the field is being used for aggregation
    * @see [[Aggregation]]
    */
  def isAggregate: Boolean = false

}

/**
  * Named Expression Companion
  * @author lawrence.daniels@gmail.com
  */
object NamedExpression {
  private[this] val idTicker = new AtomicInteger()

  /**
    * For pattern matching
    * @param expression the given [[NamedExpression]]
    */
  def unapply(expression: NamedExpression): Option[(String, Option[String])] = {
    val alias = expression match {
      case aliasable: Aliasable => aliasable.alias
      case _ => None
    }
    Option(expression.name -> alias)
  }

  /**
    * Returns a random name of the specified length
    * @return the random name
    */
  def randomName: String = s"_c${idTicker.incrementAndGet()}"

  /**
    * Named Expression Extensions
    * @param expression the given [[Expression expression]]
    */
  final implicit class NamedExpressionExtensions(val expression: Expression) extends AnyVal {
    @inline def getName: String = expression match {
      case NamedExpression(name, alias) => alias || name
      case _ => randomName
    }
  }
}