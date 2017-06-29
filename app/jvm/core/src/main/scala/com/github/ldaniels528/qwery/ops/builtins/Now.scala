package com.github.ldaniels528.qwery.ops.builtins

import java.util.Date

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Now() - Returns the current date
  * @author lawrence.daniels@gmail.com
  */
trait Now extends InternalFunction {
  override def evaluate(scope: Scope): Option[Date] = Some(new Date())
}


/**
  * Now() function
  * @author lawrence.daniels@gmail.com
  */
object Now extends Now