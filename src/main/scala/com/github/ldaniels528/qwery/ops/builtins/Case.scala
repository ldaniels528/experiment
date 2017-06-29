package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.builtins.Case.When
import com.github.ldaniels528.qwery.ops.{Condition, Expression, Scope}
import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Represents a CASE expression
  *
  * Syntax 1:
  * @example
  * {{{
  * CASE primary-expr
  *   WHEN expr1 THEN result1
  *   WHEN expr2 THEN result2
  *   ELSE expr3
  * END
  * }}}
  *
  * Syntax 2:
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
case class Case(conditions: List[When], otherwise: Option[Expression]) extends Expression {
  override def evaluate(scope: Scope): Option[Any] = {
    val expr = conditions.find(_.condition.isSatisfied(scope)).map(_.result) ?? otherwise
    expr.flatMap(_.evaluate(scope))
  }
}

/**
  * Case Singleton
  * @author lawrence.daniels@gmail.com
  */
object Case {

  /**
    * Represents a WHEN condition
    * @param condition the given [[Condition condition]]
    * @param result    the given [[Expression result expression]]
    */
  case class When(condition: Condition, result: Expression)

}

