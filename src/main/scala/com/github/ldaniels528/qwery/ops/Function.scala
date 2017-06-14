package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.Function.{RETURN_VALUE, populateArgs}

/**
  * Represents a user-defined function
  * @author lawrence.daniels@gmail.com
  */
case class Function(name: String, parameters: Seq[Field], executable: Executable) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    scope += this
    ResultSet.affected()
  }

  def invoke(scope: Scope, args: Seq[Expression]): Option[Any] = {
    // create a local scope, and populate it with the argument values as variables
    val myScope = LocalScope(scope, row = Nil)
    populateArgs(myScope, parameters, args)

    // execute the function
    executable.execute(myScope)
    myScope.lookupVariable(RETURN_VALUE).flatMap(_.value)
  }

}

/**
  * Function Companion
  * @author lawrence.daniels@gmail.com
  */
object Function {
  val RETURN_VALUE = "@@RETURN_VALUE"

  def populateArgs(scope: Scope, parameters: Seq[Field], args: Seq[Expression]): Unit = {
    // validate the argument/parameter count
    if (parameters.length != args.length) {
      throw new IllegalArgumentException(s"Mismatched arguments (expected ${parameters.length}, found ${args.length})")
    }

    // create a local scope, and populate it with the argument values as variables
    val values = args.map(_.evaluate(scope))
    parameters zip values foreach { case (param, value) =>
      scope += Variable(param.name, value)
    }
  }

}