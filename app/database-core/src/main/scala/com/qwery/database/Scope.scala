package com.qwery.database

import com.qwery.database.Scope._
import com.qwery.database.models.KeyValues

import scala.collection.concurrent.TrieMap

/**
 * Creates a new scope
 */
class Scope() {
  private val variables = TrieMap[String, Variable]()
  private var row: Option[KeyValues] = None

  /**
   * @return the current [[KeyValues row]]
   */
  def currentRow: Option[KeyValues] = row

  /**
   * Sets the current row
   * @param row the [[KeyValues row]] to set
   */
  def currentRow_=(row: Option[KeyValues]): Unit = this.row = row

  /**
   * Retrieves a named value from the scope
   * @param name the name of the field/attribute
   * @return the option of a value
   */
  def get(name: String): Option[Any] = currentRow.flatMap(_.get(name))

  /**
   * Adds a variable to the scope
   * @param variable the [[Variable variable]]
   */
  def addVariable(variable: Variable): Unit = variables(variable.name) = variable

  /**
   * Retrieves a variable by name
   * @param name the variable name
   * @return the [[Variable]]
   */
  def getVariable(name: String): Option[Variable] = variables.get(name)

}

/**
 * Scope Companion
 */
object Scope {

  /**
   * Creates a new Scope
   * @return the [[Scope]]
   */
  def apply() = new Scope()

  def replaceTags(text: String)(implicit scope: Scope): String = {
    val sb = new StringBuilder(text)
    var last = 0
    var isDone = false

    def getFieldValue(name: String, field: String): String = {
      val variable = scope.getVariable(name).getOrElse(throw new RuntimeException(s"Variable '$name' not found"))
      variable.value match {
        case Some(m: Map[String, Any]) => m.get(field).map(_.toString).getOrElse("")
        case Some(v) => v.toString
        case None => ""
      }
    }

    // replace all tags (e.g. "{{item.name}}")
    do {
      // attempt to find a tag
      val start = sb.indexOf("{{", last)
      val end = sb.indexOf("}}", start)

      isDone = start == -1 || start > end
      if (!isDone) {
        // extract the tag's contents and parse the property (name and field)
        val tag = sb.substring(start + 2, end).trim
        val (name, field) = tag.indexOf('.') match {
          case -1 => die(s"Property expected near '$tag'")
          case index => (tag.substring(0, index).trim, tag.substring(index + 1).trim)
        }

        // replace the tag with the value
        sb.replace(start, end + 2, getFieldValue(name, field))
      }
      last = start
    } while (!isDone)
    sb.toString()
  }

  /**
   * Represents a variable
   * @param name  the name of the variable
   * @param value the value of the variable
   */
  case class Variable(name: String, var value: Option[Any])

}