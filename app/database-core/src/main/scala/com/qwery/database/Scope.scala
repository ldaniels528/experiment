package com.qwery.database

import com.qwery.database.Scope._
import com.qwery.database.types.{QxAny, QxInt}

import scala.collection.concurrent.TrieMap

/**
 * Represents a scope
 */
trait Scope {

  /**
   * @return the current row ID
   */
  def currentRow: QxInt

  def exists(f: ((String, Any)) => Boolean): Boolean

  def forall(f: ((String, Any)) => Boolean): Boolean

  def foreach(f: ((String, Any)) => Unit): Unit

  /**
   * Retrieves a named value from the scope
   * @param name the name of the field/attribute
   * @return the option of a value
   */
  def get(name: String): Option[Any]

  /**
   * Retrieves a function by name
   * @param name the function name
   * @return the [[Function]]
   */
  def getFunction(name: String): Function

  /**
   * Retrieves a variable by name
   * @param name the variable name
   * @return the [[Variable]]
   */
  def getVariable(name: String): Variable

  def isEmpty: Boolean

  def nonEmpty: Boolean

  def keys: Seq[String]

}

/**
 * Scope Companion
 */
object Scope {

  /**
   * Creates a new Scope
   * @param items the name-value pairs of this scope
   * @return the [[Scope]]
   */
  def apply(items: (String, Any)*) = new DefaultScope(items: _*)

  /**
   * Represents a function
   */
  trait Function {
    def evaluate(args: Seq[QxAny])(implicit scope: Scope): QxAny
  }

  /**
   * Represents a aggregate function
   */
  trait AggFunction extends Function {
    def update(keyValues: KeyValues)(implicit scope: Scope): Unit
  }

  /**
   * Represents a variable
   * @param name  the name of the variable
   * @param value the value of the variable
   */
  case class Variable(name: String, var value: Option[Any])

}

/**
 * Creates a new Scope
 * @param items the name-value pairs of this scope
 */
class DefaultScope(items: (String, Any)*) extends Scope {
  private val mappings = Map(items: _*)
  private val functions = TrieMap[String, Function]()
  private val variables = TrieMap[String, Variable]()

  override def currentRow: QxInt = QxInt(rowID)

  override def exists(f: ((String, Any)) => Boolean): Boolean = mappings.exists(f)

  override def forall(f: ((String, Any)) => Boolean): Boolean = mappings.forall(f)

  override def foreach(f: ((String, Any)) => Unit): Unit = mappings.foreach(f)

  override def get(name: String): Option[Any] = mappings.get(name)

  def getOrElse(name: String, default: Any): Any = mappings.getOrElse(name, default)

  override def getFunction(name: String): Function = {
    functions.getOrElse(name, throw new RuntimeException(s"Function '$name' not found"))
  }

  override def getVariable(name: String): Variable = {
    variables.getOrElse(name, throw new RuntimeException(s"Variable '$name' not found"))
  }

  override def isEmpty: Boolean = mappings.isEmpty

  override def nonEmpty: Boolean = mappings.nonEmpty

  override def keys: Seq[String] = mappings.keys.toSeq

  def toList: List[(String, Any)] = mappings.toList

  def toMap: Map[String, Any] = mappings

  def rowID: Option[ROWID] = mappings.collectFirst { case (name, id: ROWID) if name == ROWID_NAME => id }

  override def toString: String = mappings.toString

  def values: Seq[Any] = mappings.values.toSeq

}