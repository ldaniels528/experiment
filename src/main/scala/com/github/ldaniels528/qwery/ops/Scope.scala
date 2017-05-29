package com.github.ldaniels528.qwery.ops

import scala.collection.concurrent.TrieMap

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {
  private lazy val functions = TrieMap[String, Function]()
  private lazy val variables = TrieMap[String, Variable]()
  private lazy val views = TrieMap[String, View]()

  /**
    * Returns the row that scope is currently referencing
    * @return a row of data
    */
  def row: Row

  /**
    * Returns a column value by name
    * @param name the name of the desired column
    * @return the option of a value
    */
  def get(name: String): Option[Any]

  /////////////////////////////////////////////////////////////////
  //    Functions
  /////////////////////////////////////////////////////////////////

  /**
    * Adds a function to the scope
    * @param function the given function
    */
  def +=(function: Function): Unit = functions(function.name) = function

  /**
    * Looks up a function by name
    * @param name the name of the desired function
    * @return an option of a [[Function function]]
    */
  def lookupFunction(name: String): Option[Function] = functions.get(name)

  /////////////////////////////////////////////////////////////////
  //    Variables
  /////////////////////////////////////////////////////////////////

  /**
    * Adds a variable to the scope
    * @param variable the given variable
    */
  def +=(variable: Variable): Unit = variables(variable.name) = variable

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

  /**
    * Looks up a variable by name
    * @param name the name of the desired variable
    * @return an option of a [[Variable variable]]
    */
  def lookupVariable(name: String): Option[Variable] = variables.get(name)

  /////////////////////////////////////////////////////////////////
  //    Views
  /////////////////////////////////////////////////////////////////

  /**
    * Adds a view to the scope
    * @param view the given view
    */
  def +=(view: View): Unit = views(view.name) = view

  /**
    * Looks up a view by name
    * @param name the name of the desired view
    * @return an option of a [[View view]]
    */
  def lookupView(name: String): Option[View] = views.get(name)

}
