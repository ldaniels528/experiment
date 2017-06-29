package com.github.ldaniels528.qwery.ops.builtins

import java.util.Random

import com.github.ldaniels528.qwery.ops.Scope

/**
  * Rand() - Random Number Generator
  * @author lawrence.daniels@gmail.com
  */
object Rand extends InternalFunction {
  private lazy val random = new Random()

  override def evaluate(scope: Scope): Option[Any] = Some(random.nextDouble())

}
