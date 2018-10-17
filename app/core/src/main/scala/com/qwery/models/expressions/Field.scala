package com.qwery.models.expressions

/**
  * Represents a SQL field
  * @author lawrence.daniels@gmail.com
  */
trait Field extends NamedExpression

/**
  * Field Companion
  * @author lawrence.daniels@gmail.com
  */
object Field {

  /**
    * Returns a new field implementation
    * @param field the [[Symbol symbol]] represent a field name
    * @return the [[Field]]
    */
  def apply(field: Symbol): Field = apply(field.name)

  /**
    * Returns a new field implementation
    * @param descriptor the name (e.g. "customerId") or descriptor ("A.customerId") of the field
    * @return the [[Field]]
    */
  def apply(descriptor: String): Field = descriptor.split('.').toList match {
    case "*" :: Nil => AllFields
    case name :: Nil => BasicField(name)
    case alias :: name :: Nil => BasicField(name).as(alias)
    case _ => throw new IllegalArgumentException(s"Invalid field reference '$descriptor'")
  }

  def unapply(field: Field): Option[String] = Some(field.name)

}

/**
  * Represents the selection of all fields
  * @author lawrence.daniels@gmail.com
  */
case object AllFields extends Field {
  override val name: String = "*"
}

/**
  * Represents a generic field
  * @param name the name of the field
  */
case class BasicField(name: String) extends Field


