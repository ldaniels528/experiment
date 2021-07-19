package com.qwery.models

import com.qwery.models.expressions.Condition

/**
 * SQL WHILE ... DO statement
 * @param condition the given [[Condition condition]]
 * @param invokable the given [[Invokable invokable]]
 */
case class WhileDo(condition: Condition, invokable: Invokable) extends Invokable