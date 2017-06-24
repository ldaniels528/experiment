package com.github.ldaniels528.qwery.ops

import java.io.File

import com.github.ldaniels528.qwery.ops.sql.{Procedure, View}
import com.github.ldaniels528.qwery.util.OptionHelper._

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
  * Represents a scope
  * @author lawrence.daniels@gmail.com
  */
trait Scope extends DataContainer {
  private lazy val functions = TrieMap[String, Function]()
  private lazy val procedures = TrieMap[String, Procedure]()
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
  override def get(name: String): Option[Any] = row.get(name)

  /**
    * Returns a column-value pair by name
    * @param name the name of the desired column
    * @return the option of a column-value tuple
    */
  override def getColumn(name: String): Option[Column] = row.getColumn(name)

  /////////////////////////////////////////////////////////////////
  //    Files
  /////////////////////////////////////////////////////////////////

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

/**
  * Scope Companion
  * @author lawrence.daniels@gmail.com
  */
object Scope {

  /**
    * Creates a new scope
    * @return a new [[Scope scope]]
    */
  def apply(): Scope = DefaultScope()

  /**
    * Creates a new child scope
    * @param parent the parent scope
    * @return a new [[Scope scope]]
    */
  def apply(parent: Scope, row: Row = Nil): ChildScope = ChildScope(parent, row)

  /**
    * Creates a top-level (root) scope
    * @return a new [[Scope scope]]
    */
  def root(): Scope = DefaultScope().includeEnvVars()

  /**
    * Scope Enrichment
    * @param scope the given [[Scope scope]]
    */
  implicit class ScopeEnrichment(val scope: Scope) extends AnyVal {

    @inline
    def includeEnvVars(): scope.type = {
      // import the environment variables
      System.getenv().asScala foreach { case (name, value) =>
        scope += Variable(name = s"env.$name", value = Option(value))
      }
      scope
    }
  }

  /**
    * Represents a local ephemeral scope
    * @author lawrence.daniels@gmail.com
    */
  case class ChildScope(parent: Scope, row: Row = Nil) extends Scope {

    override def get(name: String): Option[Any] = getColumn(name).map(_._2)

    override def getColumn(name: String): Option[(String, Any)] = super.getColumn(name) ?? parent.getColumn(name)

    override def lookupFunction(name: String): Option[Function] = {
      super.lookupFunction(name) ?? parent.lookupFunction(name)
    }

    override def lookupVariable(name: String): Option[Variable] = {
      super.lookupVariable(name) ?? parent.lookupVariable(name)
    }

  }

  /**
    * Represents a simple scope
    * @author lawrence.daniels@gmail.com
    */
  case class DefaultScope(row: Row = Nil) extends Scope

}