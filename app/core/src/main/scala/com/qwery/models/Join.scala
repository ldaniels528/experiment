package com.qwery.models

import com.qwery.models.JoinTypes.JoinType
import com.qwery.models.expressions.Condition

/**
  * Represents a JOIN clause
  * @param source    the [[TableRef table]] or [[Invokable query]]
  * @param condition the [[Condition conditional expression]]
  * @param `type`    the given [[JoinType]]
  */
case class Join(source: Invokable, condition: Condition, `type`: JoinType)
