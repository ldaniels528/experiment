package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Represents a power operation
  * @param a the left quantity
  * @param b the right quantity
  */
case class Pow(a: Expression, b: Expression) extends Expression {
  override def evaluate(scope: Scope): Option[Double] = for {
    x <- a.getAsDouble(scope)
    y <- b.getAsDouble(scope)
  } yield Math.pow(x, y)
}
