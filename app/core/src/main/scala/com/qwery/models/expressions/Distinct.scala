package com.qwery.models.expressions

/**
  * Represents a Distinct expression
  * @param expressions the given [[Expression]]s for which to aggregate the distinct values
  * @author lawrence.daniels@gmail.com
  */
case class Distinct(expressions: List[Expression]) extends Expression

/**
  * Distinct Companion
  * @author lawrence.daniels@gmail.com
  */
object Distinct {

  /**
    * Creates a new Distinct expression
    * @param expressions the given [[Expression]]s for which to aggregate the distinct values
    * @return a new [[Distinct distinct]] expression
    */
  def apply(expressions: Expression*): Distinct = new Distinct(expressions.toList)

}