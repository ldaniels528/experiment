package com.qwery.models

import com.qwery.models.expressions.Condition

/**
  * SQL WHILE statement
  * @param condition the given [[Condition]]
  */
case class While(condition: Condition, invokable: Invokable) extends Invokable