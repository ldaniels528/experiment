package com.qwery.models
package expressions

import com.qwery.die

/**
  * Represents a SQL field reference
  * @author lawrence.daniels@gmail.com
  */
trait FieldRef extends NamedExpression

/**
  * Field Reference Companion
  * @author lawrence.daniels@gmail.com
  */
object FieldRef {

  /**
    * Returns a new field implementation
    * @param field the [[Symbol symbol]] represent a field name
    * @return the [[FieldRef field reference]]
    */
  def apply(field: Symbol): FieldRef = apply(field.name)

  /**
    * Returns a new field implementation
    * @param descriptor the name (e.g. "customerId") or descriptor ("A.customerId") of the field reference
    * @return the [[FieldRef field reference]]
    */
  def apply(descriptor: String): FieldRef = descriptor.split('.').toList match {
    case "*" :: Nil => AllFields
    case name :: Nil => BasicFieldRef(name)
    case alias :: name :: Nil => JoinFieldRef(name, tableAlias = Some(alias))
    case _ => die(s"Invalid field descriptor '$descriptor'")
  }

  def unapply(field: FieldRef): Option[String] = Some(field.name)

}

/**
  * Represents the selection of all fields
  * @author lawrence.daniels@gmail.com
  */
case object AllFields extends FieldRef {
  override val name: String = "*"
}

/**
  * Represents a generic field
  * @param name the name of the [[FieldRef field reference]]
  */
case class BasicFieldRef(name: String) extends FieldRef

/**
  * Represents a join field
  * @param name       the name of the [[FieldRef field reference]]
  * @param tableAlias the given table alias
  */
case class JoinFieldRef(name: String, tableAlias: Option[String] = None) extends FieldRef
