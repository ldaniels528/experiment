package com.github.ldaniels528.qwery.ops

/**
  * Represents a function reference
  * @author lawrence.daniels@gmail.com
  */
case class FunctionRef(name: String) extends Evaluatable {

  override def compare(that: Evaluatable, scope: Scope): Int = ???

  override def evaluate(scope: Scope): Option[Any] = ???

}