package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.Token

/**
  * Represents a field reference
  * @author lawrence.daniels@gmail.com
  */
trait Field extends NamedExpression

/**
  * Field Companion
  * @author lawrence.daniels@gmail.com
  */
object Field {

  /**
    * Creates a new field
    * @param name the name of the field
    * @return a new [[Field field]] instance
    */
  def apply(name: String) = BasicField(name)

  /**
    * Creates a new field from a token
    * @param token the given [[Token token]]
    * @return a new [[Field field]] instance
    */
  def apply(token: Token): Field = token.text match {
    case "*" => AllFields
    case name => BasicField(name)
  }

  /**
    * For pattern matching
    */
  def unapply(field: Field): Option[String] = Some(field.name)

}

/**
  * Represents a reference to all fields in a specific collection
  */
object AllFields extends BasicField(name = "*")

/**
  * Represents an Aggregate Field
  * @author lawrence.daniels@gmail.com
  */
case class AggregateField(name: String) extends Field with Aggregation {
  private var value: Option[Any] = None

  override def evaluate(scope: Scope): Option[Any] = value

  override def update(scope: Scope): Unit = value = scope.get(name)
}

/**
  * Represents a field reference
  * @author lawrence.daniels@gmail.com
  */
case class BasicField(name: String) extends Field {
  override def evaluate(scope: Scope): Option[Any] = scope.get(name)
}

