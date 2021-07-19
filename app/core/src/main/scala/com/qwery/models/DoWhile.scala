package com.qwery.models

import com.qwery.models.expressions.Condition

/**
 * SQL DO ... WHILE statement
 * @param invokable the given [[Invokable invokable]]
 * @param condition the given [[Condition condition]]
 */
case class DoWhile(invokable: Invokable, condition: Condition) extends Invokable