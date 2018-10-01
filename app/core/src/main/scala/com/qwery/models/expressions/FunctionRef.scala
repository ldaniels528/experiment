package com.qwery.models.expressions

/**
  * Represents a reference to a function
  * @param name the name of the function
  * @param args the function-call arguments
  */
case class FunctionRef(name: String, args: Seq[Expression]) extends Field