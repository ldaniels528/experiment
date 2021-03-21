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
  * Represents a reference to a row-set variable
  * @param name the name of the variable
  */
case class RowSetVariableRef(name: String) extends VariableRef with Queryable

/**
 * Represents a reference to a scalar variable
 * @param name the name of the variable
 */
case class ScalarVariableRef(name: String) extends VariableRef with FieldRef
