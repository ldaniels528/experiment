package com.github.ldaniels528.qwery.ops

/**
  * Represents a function reference
  * @author lawrence.daniels@gmail.com
  */
case class FunctionRef(name: String) extends Value {

  override def compare(that: Value, scope: Scope): Int = ???

  override def evaluate(scope: Scope): Option[Any] = ???

}