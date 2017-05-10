package com.github.ldaniels528.qwery.ops

/**
  * Represents a function
  * @author lawrence.daniels@gmail.com
  */
trait Function {

  def name: String

  def invoke(scope: Scope, args: Seq[Value]): Option[Any]

}
