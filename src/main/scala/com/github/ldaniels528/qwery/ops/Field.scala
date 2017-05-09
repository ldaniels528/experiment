package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.Token

/**
  * Represents a field reference
  * @author lawrence.daniels@gmail.com
  */
case class Field(name: String) extends Evaluatable {

  override def compare(that: Evaluatable, scope: Scope): Int = {
    scope.get(name).map(v => Evaluatable(v).compare(that, scope)) getOrElse -1
  }

  override def evaluate(scope: Scope): Option[Any] = scope.get(name)

}

/**
  * Field Companion
  * @author lawrence.daniels@gmail.com
  */
object Field {

  /**
    * Creates a new field from a token
    * @param token the given [[Token token]]
    * @return a new [[Field field]] instance
    */
  def apply(token: Token): Field = Field(token.text)

  /**
    * Represents a reference to all fields in a specific collection
    */
  object AllFields extends Field(name = "*")

}
