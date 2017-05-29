package com.github.ldaniels528.qwery.ops

/**
  * Represents a Variable Assignment
  * @author lawrence.daniels@gmail.com
  */
case class Assignment(variableRef: VariableRef, value: Expression) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    val variable = scope.lookupVariable(variableRef.name)
      .getOrElse(throw new IllegalArgumentException(s"No such variable '${variableRef.name}'"))
    ResultSet(rows = Iterator(Seq(variableRef.name -> variable.set(scope, value).orNull)))
  }

}
