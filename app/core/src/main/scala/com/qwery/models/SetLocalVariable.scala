package com.qwery.models

import com.qwery.models.expressions.Expression

/**
  * Local Variable assignment
  * @param name       the given variable name
  * @param expression the given [[Expression expression]]
  */
case class SetLocalVariable(name: String, expression: Expression) extends Invokable