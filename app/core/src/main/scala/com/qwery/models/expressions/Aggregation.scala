package com.qwery.models.expressions

/**
  * Represents an aggregation function
  * @author lawrence.daniels@gmail.com
  */
trait Aggregation extends NamedExpression {
  override val isAggregate = true
}