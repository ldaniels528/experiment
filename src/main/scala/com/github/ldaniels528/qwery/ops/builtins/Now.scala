package com.github.ldaniels528.qwery.ops.builtins

import java.util.Date

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Now() function
  */
object Now extends InternalFunction0 {
  override def evaluate(scope: Scope): Option[Date] = Some(new Date())
}
