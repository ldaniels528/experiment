package com.github.ldaniels528.qwery.ops

/**
  * Represents a function
  * @author lawrence.daniels@gmail.com
  */
trait Function {

  def name: String

  def params: Seq[String] = Nil

  def invoke(scope: Scope, args: Seq[Expression]): Option[Any]

  override def toString: String = s"$name(${params.mkString(", ")})"

}
