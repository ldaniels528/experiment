package com.github.ldaniels528.qwery.ops

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {

  def data: Row

  def get(name: String): Option[Any]

  def lookup(ref: FunctionRef): Option[Function]

  def lookupVariable(name: String): Option[Variable]

}
