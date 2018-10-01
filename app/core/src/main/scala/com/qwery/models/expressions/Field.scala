package com.qwery.models.expressions

import com.qwery.models.{Aliasable, Invokable}

/**
  * Represents a SQL field
  * @author lawrence.daniels@gmail.com
  */
trait Field extends NamedExpression with Invokable with Aliasable {

  /**
    * @return the name of the field
    */
  def name: String

  /**
    * @return indicates whether the field is being used for aggregation
    */
  def isAggregate: Boolean = false

}

/**
  * Field Companion
  * @author lawrence.daniels@gmail.com
  */
object Field {

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

  /**
    * Returns a new constant field
    * @param name the name of the field
    * @return the [[ConstantField]]
    */
  def apply(name: String, value: Expression): ConstantField = ConstantField(value).as(name)

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

/**
  * Represents a field populated with a fixed-value
  * @param value the [[Expression]] resulting the constant value
  */
case class ConstantField(value: Expression) extends Field {
  lazy val name: String = alias.getOrElse(NamedExpression.randomName)
}


