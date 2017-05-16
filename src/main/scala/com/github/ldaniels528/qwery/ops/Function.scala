package com.github.ldaniels528.qwery.ops

/**
  * Represents a user-defined function
  * @author lawrence.daniels@gmail.com
  */
trait Function {

  def name: String

  def params: Seq[String] = Nil

  def invoke(scope: Scope, args: Seq[Expression]): Option[Any]

}
