package com.qwery.models.expressions

/**
  * Represents a function invocation
  * @param name the name of the function
  * @param args the function-call arguments
  */
case class FunctionCall(name: String, args: List[Expression]) extends NamedExpression

/**
  * FunctionCall Companion
  * @author lawrence.daniels@gmail.com
  */
object FunctionCall {

  /**
    * Creates a new function call
    * @param name the name of the function
    * @param args the function-call arguments
    * @return a [[FunctionCall]]
    */
  def apply(name: String)(args: Expression*) = new FunctionCall(name, args.toList)

}
