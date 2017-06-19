package com.github.ldaniels528.qwery.ops

import java.io.File

import com.github.ldaniels528.qwery.ops.sql.{Procedure, View}

import scala.collection.concurrent.TrieMap

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope {
  private lazy val functions = TrieMap[String, Function]()
  private lazy val procedures = TrieMap[String, Procedure]()
  private lazy val variables = TrieMap[String, Variable]()
  private lazy val views = TrieMap[String, View]()
  private lazy val resources = TrieMap[String, Row]()

  /**
    * Returns the row that scope is currently referencing
    * @return a row of data
    */
  def row: Row // = row("")

  /**
    * Returns the row that scope is currently referencing
    * @return a row of data
    */
  def row(alias: String): Option[Row] = resources.get(alias)

  /**
    * Sets an aliased row of data
    * @param alias the data resource alias (e.g. "FROM 'companylist.csv' AS A")
    * @param row   the row of data
    */
  def setRow(alias: String, row: Row): Unit = resources(alias) = row

  /**
    * Returns a column value by name
    * @param name the name of the desired column
    * @return the option of a value
    */
  def get(name: String): Option[Any] = row.find(_._1.equalsIgnoreCase(name)).map(_._2)

  /**
    * Returns the files in the currently directory
    * @return a list of files
    */
  def getFiles(file: File = new File(".")): List[File] = {
    file match {
      case f if f.isDirectory => f.listFiles().flatMap(getFiles).toList
      case f => f :: Nil
    }
  }

  /////////////////////////////////////////////////////////////////
  //    Functions
  /////////////////////////////////////////////////////////////////

  /**
    * Adds a function to the scope
    * @param function the given function
    */
  def +=(function: Function): Unit = functions(function.name) = function

  /**
    * Returns the collection of functions tied to this scope
    * @return a collection of functions
    */
  def getFunctions: Iterable[Function] = functions.values

  /**
    * Looks up a function by name
    * @param name the name of the desired function
    * @return an option of a [[Function function]]
    */
  def lookupFunction(name: String): Option[Function] = functions.get(name)

  /////////////////////////////////////////////////////////////////
  //    Procedures
  /////////////////////////////////////////////////////////////////

  /**
    * Adds a procedure to the scope
    * @param procedure the given procedure
    */
  def +=(procedure: Procedure): Unit = procedures(procedure.name) = procedure

  /**
    * Returns the collection of procedures tied to this scope
    * @return a collection of procedures
    */
  def getProcedures: Iterable[Procedure] = procedures.values

  /**
    * Looks up a procedure by name
    * @param name the name of the desired procedure
    * @return an option of a [[Procedure procedure]]
    */
  def lookupProcedure(name: String): Option[Procedure] = procedures.get(name)

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
    * Returns the collection of variables tied to this scope
    * @return a collection of variables
    */
  def getVariables: Iterable[Variable] = variables.values

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
    * Returns the collection of views tied to this scope
    * @return a collection of views
    */
  def getViews: Iterable[View] = views.values

  /**
    * Looks up a view by name
    * @param name the name of the desired view
    * @return an option of a [[View view]]
    */
  def lookupView(name: String): Option[View] = views.get(name)

}
