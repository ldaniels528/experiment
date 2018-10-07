package com.qwery.models.expressions

import com.qwery.models.expressions.Case.When

/**
  * Represents a CASE expression
  * @example
  * {{{
  * CASE primary-expr
  *   WHEN expr1 THEN result1
  *   WHEN expr2 THEN result2
  *   ELSE expr3
  * END
  * }}}
  * @example
  * {{{
  * CASE
  *   WHEN value1 = expr1 THEN result1
  *   WHEN value2 = expr2 THEN result2
  *   ELSE expr3
  * END
  * }}}
  * @param conditions the list of WHEN conditions
  */
case class Case(conditions: List[When], otherwise: Option[Expression]) extends Expression

/**
  * Case Companion
  * @author lawrence.daniels@gmail.com
  */
object Case {

  /**
    * Creates a new Case instance
    * @param conditions the given collection of [[When]] cases
    * @param otherwise  the optional else [[Expression]]
    * @return the [[Case]]
    */
  def apply(conditions: When*)(otherwise: Option[Expression]): Case = new Case(conditions.toList, otherwise)

  /**
    * Represents a WHEN condition
    * @param condition the given [[Condition condition]]
    * @param result    the given [[Expression result expression]]
    */
  case class When(condition: Condition, result: Expression)

}
