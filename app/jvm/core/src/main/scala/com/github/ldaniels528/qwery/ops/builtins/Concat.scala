package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Concat - concatenates two strings
  * @param a the left string
  * @param b the right string
  */
case class Concat(a: Expression, b: Expression) extends Expression {
  override def evaluate(scope: Scope): Option[String] = for {
    x <- a.getAsString(scope)
    y <- b.getAsString(scope)
  } yield x + y

}
