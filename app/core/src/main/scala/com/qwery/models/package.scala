package com.qwery

import com.qwery.models.expressions._

/**
  * models package object
  * @author lawrence.daniels@gmail.com
  */
package object models {

  /**
    * Shortcut for creating a scalar variable reference
    * @param name the name of the variable
    * @return a new [[ScalarVariableRef scalar variable reference]]
    */
  def $$(name: String) = ScalarVariableRef(name)

  /**
    * Shortcut for creating a row-set variable reference
    * @param name the name of the variable
    * @return a new [[RowSetVariableRef row-set variable reference]]
    */
  def @@(name: String) = RowSetVariableRef(name)

  /**
    * Invokable Conversions
    * @param invokable the given [[Invokable invokable]]
    */
  final implicit class InvokableConversions(val invokable: Invokable) extends AnyVal {
    @inline def toQueryable: Queryable = invokable match {
      case q: Queryable => q
      case x => die(s"Invalid input source '$x'")
    }
  }

}
