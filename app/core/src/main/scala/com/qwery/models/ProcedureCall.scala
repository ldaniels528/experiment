package com.qwery.models

import com.qwery.models.expressions.Expression

/**
  * Invokes a procedure by name
  * @param name the name of the procedure
  * @param args the collection of [[Expression arguments]] to be passed to the procedure upon invocation
  */
case class ProcedureCall(name: String, args: List[Expression]) extends Invokable