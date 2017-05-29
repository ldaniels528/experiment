package com.github.ldaniels528.qwery.ops

/**
  * Represents a Variable Declaration
  * @author lawrence.daniels@gmail.com
  */
case class Declare(variableRef: VariableRef, `type`: String) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    scope += Variable(variableRef.name, value = None)
    ResultSet.ok()
  }

}
