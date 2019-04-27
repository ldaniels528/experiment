package com.qwery.models

import com.qwery.models.expressions.VariableRef

/**
  * SQL DECLARE
  * @param variable   the variable for which to declare
  * @param `type`     the variable type
  * @param isExternal indicates whether the variable is externally defined
  */
case class Declare(variable: VariableRef, `type`: String, isExternal: Boolean) extends Invokable