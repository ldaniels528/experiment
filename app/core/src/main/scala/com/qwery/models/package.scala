package com.qwery

import com.qwery.models.expressions._

/**
  * models package object
  * @author lawrence.daniels@gmail.com
  */
package object models {

  /**
    * Shortcut for creating a local variable reference
    * @param name the name of the variable
    * @return a new [[LocalVariableRef local variable reference]]
    */
  def @@(name: String) = LocalVariableRef(name)

  /**
    * Shortcut for creating a row-set variable reference
    * @param name the name of the variable
    * @return a new [[RowSetVariableRef row-set variable reference]]
    */
  def @#(name: String) = RowSetVariableRef(name)

  /**
    * Invokable Conversions
    * @param invokable the given [[Invokable invokable]]
    */
  final implicit class InvokableConversions(val invokable: Invokable) extends AnyVal {
    @inline def toQueryable: Queryable = invokable match {
      case q: Queryable => q
      case x => throw new IllegalArgumentException(s"Invalid input source '$x'")
    }
  }

}
