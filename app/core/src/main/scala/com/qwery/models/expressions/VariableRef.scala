package com.qwery.models
package expressions

/**
  * Represents a reference to a variable
  * @author lawrence.daniels@gmail.com
  */
sealed trait VariableRef {

  /**
    * @return the name of the variable
    */
  def name: String
}

/**
  * Variable Reference Companion
  * @author lawrence.daniels@gmail.com
  */
object VariableRef {

  /**
    * Creates a new variable reference.
    * Variables starting with '$' will result in a [[LocalVariableRef local variable reference]].
    * Variables starting with '@' will result in a [[RowSetVariableRef row-set variable reference]].
    * @param name the given variable name
    * @return the [[VariableRef]]
    */
  def apply(name: String): VariableRef = {
    name match {
      case s if s.startsWith("$") => LocalVariableRef(s.drop(1))
      case s if s.startsWith("@") => RowSetVariableRef(s.drop(1))
      case _ => die(s"Invalid variable reference '$name'")
    }
  }

}

/**
  * Represents a reference to a local variable
  * @param name the name of the variable
  */
case class LocalVariableRef(name: String) extends VariableRef with NamedExpression

/**
  * Represents a reference to a row-set variable
  * @param name the name of the variable
  */
case class RowSetVariableRef(name: String) extends VariableRef with Invokable with Aliasable

