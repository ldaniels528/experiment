package com.github.ldaniels528.qwery.ops

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {

  def get(name: String): Option[Any]

  def update(data: Map[String, Any]): Unit

}
