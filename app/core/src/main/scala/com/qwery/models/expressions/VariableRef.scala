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
  * Represents a reference to a local variable
  * @param name the name of the variable
  */
case class LocalVariableRef(name: String) extends VariableRef with NamedExpression

/**
  * Represents a reference to a row-set variable
  * @param name the name of the variable
  */
case class RowSetVariableRef(name: String) extends VariableRef with Invokable with Aliasable

/**
  * Local Variable assignment
  * @param name       the given variable name
  * @param expression the given [[Expression expression]]
  */
case class SetLocalVariable(name: String, expression: Expression) extends Invokable

/**
  * Row Variable assignment
  * @param name    the given variable name
  * @param dataset the given [[Invokable dataset]]
  */
case class SetRowVariable(name: String, dataset: Invokable) extends Invokable
