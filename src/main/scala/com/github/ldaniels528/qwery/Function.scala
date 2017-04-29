package com.github.ldaniels528.qwery

/**
  * Represents a function
  * @author lawrence.daniels@gmail.com
  */
case class Function() extends Evaluatable {

  override def compare(that: Evaluatable, data: Map[String, Any]): Int = ???

  override def evaluate(data: Map[String, Any]): Option[Any] = ???

}