package com.github.ldaniels528.qwery.ops.math

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Represents a multiplication operation
  * @param a the left quantity
  * @param b the right quantity
  */
case class Multiply(a: Expression, b: Expression) extends Expression {
  override def evaluate(scope: Scope): Option[Double] = for {
    x <- a.getAsDouble(scope)
    y <- b.getAsDouble(scope)
  } yield x * y

  override def toSQL: String = s"${a.toSQL} * ${b.toSQL}"
}