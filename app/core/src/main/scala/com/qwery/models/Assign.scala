package com.qwery.models

import com.qwery.models.expressions.VariableRef

/**
  * Variable assignment
  * @param variable the given [[VariableRef]]
  * @param value the given [[Invokable]]
  */
case class Assign(variable: VariableRef, value: Invokable) extends Invokable