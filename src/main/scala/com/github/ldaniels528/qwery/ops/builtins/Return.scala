package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.Function.RETURN_VALUE
import com.github.ldaniels528.qwery.ops.{Executable, Expression, NamedExpression, ResultSet, Scope, Variable}

/**
  * Returns a return operation
  * @param expression the expression to evaluate and return (if executed from a function)
  */
case class Return(expression: Option[Expression]) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val returnValue = expression.map(_.evaluate(scope))
    scope += Variable(RETURN_VALUE, returnValue)
    ResultSet(rows = Iterator(Seq(NamedExpression.randomName() -> returnValue.orNull)))
  }

}
