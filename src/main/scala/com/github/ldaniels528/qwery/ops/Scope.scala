package com.github.ldaniels528.qwery.ops

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {

  def data: Seq[(String, Any)]

  def get(name: String): Option[Any]

  def getName(value: Value): String

}