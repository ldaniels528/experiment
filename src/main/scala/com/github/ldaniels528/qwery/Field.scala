package com.github.ldaniels528.qwery

/**
  * Represents a field reference
  * @author lawrence.daniels@gmail.com
  */
case class Field(name: String) extends Evaluatable {

  override def compare(that: Evaluatable, data: Map[String, Any]): Int = {
    data.get(name).map(v => Evaluatable.apply(v).compare(that, data)) getOrElse -1
  }

  override def evaluate(data: Map[String, Any]): Option[Any] = data.get(name)
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
