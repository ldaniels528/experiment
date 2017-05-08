package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.Evaluatable

/**
  * Represents a function reference
  * @author lawrence.daniels@gmail.com
  */
case class FunctionRef(name: String) extends Evaluatable {

  override def compare(that: Evaluatable, data: Map[String, Any]): Int = ???

  override def evaluate(data: Map[String, Any]): Option[Any] = ???

}