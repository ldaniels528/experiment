package com.github.ldaniels528.qwery.ops.math

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Represents an concatenation operation
  * @param a the left quantity
  * @param b the right quantity
  */
case class Concat(a: Expression, b: Expression) extends Expression {
  override def evaluate(scope: Scope): Option[String] = for {
    x <- a.getAsString(scope)
    y <- b.getAsString(scope)
  } yield x + y

  override def toSQL: String = s"$a | $b"
}
