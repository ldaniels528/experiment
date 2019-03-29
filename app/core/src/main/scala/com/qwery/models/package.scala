package com.qwery

import com.qwery.models.expressions.{LocalVariableRef, RowSetVariableRef}

/**
  * models package object
  * @author lawrence.daniels@gmail.com
  */
package object models {

  /**
    * Shortcut for creating a local variable reference
    * @param name the name of the variable
    * @return a new [[LocalVariableRef]]
    */
  def $(name: String) = LocalVariableRef(name)

  /**
    * Shortcut for creating a row-set variable reference
    * @param name the name of the variable
    * @return a new [[RowSetVariableRef]]
    */
  def @@(name: String) = RowSetVariableRef(name)

}
