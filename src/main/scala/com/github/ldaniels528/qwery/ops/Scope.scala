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

  /////////////////////////////////////////////////////////////////
  //    Functions
  /////////////////////////////////////////////////////////////////

  def +=(function: Function): Unit = functions(function.name) = function

  def lookupFunction(name: String): Option[Function] = functions.get(name)

  /////////////////////////////////////////////////////////////////
  //    Variables
  /////////////////////////////////////////////////////////////////

  def +=(variable: Variable): Unit = variables(variable.name) = variable

  def env(name: String): String = {
    lookupVariable(name).flatMap(_.value).map(_.toString)
      .getOrElse(throw new IllegalStateException(s"Environment variable '$name' is required"))
  }

  /**
    * Expands a handle bars expression
    * @param template the given handle bars expression (e.g. "{{ inbox.file }}")
    * @return the string result of the expansion
    */
  def expand(template: String): String = {
    val sb = new StringBuilder(template)
    var start: Int = 0
    var end: Int = 0
    var done = false
    do {
      start = sb.indexOf("{{")
      end = sb.indexOf("}}")
      done = start == -1 || end < start
      if (!done) {
        val expr = sb.substring(start + 2, end).trim
        sb.replace(start, end + 2, lookupVariable(expr).flatMap(_.value).map(_.toString).getOrElse(""))
      }
    } while (!done)
    sb.toString
  }

  def lookupVariable(name: String): Option[Variable] = variables.get(name)

}
