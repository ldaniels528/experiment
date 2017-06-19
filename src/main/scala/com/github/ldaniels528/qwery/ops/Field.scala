package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.TokenStream
import com.github.ldaniels528.qwery.util.OptionHelper._

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
    * @return a new [[BasicField field]] instance
    */
  def apply(name: String) = BasicField(name)

  /**
    * Creates a new fixed-width field
    * @param name the name of the field
    * @return a new [[FixedWidth field]] instance
    */
  def apply(name: String, width: Int): Field with FixedWidth = {
    new BasicField(name) with FixedWidth {
      val width: Int = width
    }
  }

  /**
    * Creates a new field from a token
    * @param tokenStream the given [[TokenStream token stream]]
    * @return a new [[Field field]] instance
    */
  def apply(tokenStream: TokenStream): Field = {
    tokenStream match {
      case ts if ts nextIf "*" => AllFields
      case ts if ts.peekAhead(1).exists(_.text == "^") =>
        val name = ts.next().text
        ts.expect("^")
        val width = ts.next().text.toDouble.toInt
        apply(name, width)
      case ts => BasicField(ts.next().text)
    }
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
  * Represents a simple field definition
  * @author lawrence.daniels@gmail.com
  */
case class BasicField(name: String) extends Field {

  override def evaluate(scope: Scope): Option[Any] = scope.get(name)

}

/**
  * Represents a fixed-width trait
  * @author lawrence.daniels@gmail.com
  */
trait FixedWidth extends Field {

  def width: Int

}

/**
  * Represents a join field definition
  * @param alias      the data resource (e.g. table) alias
  * @param columnName the name of the column being referenced
  * @see Field
  */
case class JoinField(alias: String, columnName: String) extends Field {
  val name = s"$alias.$columnName"

  override def evaluate(scope: Scope): Option[Any] = {
    (scope.getTuple(name) ?? scope.getTuple(columnName)).map(_._2)
  }

}