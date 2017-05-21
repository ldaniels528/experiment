package com.github.ldaniels528.qwery.ops

import scala.collection.concurrent.TrieMap

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {
  private lazy val functions = TrieMap[String, Function]()
  private lazy val variables = TrieMap[String, Variable]()

  def row: Row

  def get(name: String): Option[Any]

  def +=(function: Function): Unit = functions(function.name) = function

  def +=(variable: Variable): Unit = variables(variable.name) = variable

  def lookupFunction(name: String): Option[Function] = functions.get(name)

  def lookupVariable(name: String): Option[Variable] = variables.get(name)

}
