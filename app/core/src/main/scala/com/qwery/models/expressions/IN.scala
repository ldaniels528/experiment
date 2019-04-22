package com.qwery.models.expressions

import com.qwery.models.{Invokable, Queryable}

/**
  * Creates an IN clause
  * @param expr   the given field/expression for which to compare
  * @param source the source of the comparison
  * @author lawrence.daniels@gmail.com
  */
case class IN(expr: Expression, source: Invokable) extends Condition

/**
  * IN Companion
  * @author lawrence.daniels@gmail.com
  */
object IN {

  /**
    * Creates an IN clause
    * @param expr   the given field/expression for which to compare
    * @param values the values to be used in the comparison
    * @return a new [[IN in-clause]]
    */
  def apply(expr: Expression)(values: Expression*): IN = new IN(expr, Values(values))

  /**
    * Represents a collection of values
    * @param items the collection of [[Values values]]
    */
  case class Values(items: Seq[Expression]) extends Queryable

}

