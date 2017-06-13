package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.Function.populateArgs

/**
  * Represents a Stored Procedure
  * @author Lawrence Daniels <lawrence.daniels@gmail.com>
  */
case class Procedure(name: String, parameters: Seq[Field], executable: Executable) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    scope += this
    ResultSet.ok()
  }

  def invoke(scope: Scope, args: Seq[Expression]): ResultSet = {
    // create a local scope, and populate it with the argument values as variables
    val myScope = LocalScope(scope, row = Nil)
    populateArgs(myScope, parameters, args)

    // execute the procedure
    executable.execute(myScope)
  }

}
