package com.github.ldaniels528.qwery.ops

/**
  * Represents a Field/Variable Assignment
  * @author lawrence.daniels@gmail.com
  */
case class Assignment(reference: NamedExpression, value: Expression) extends Executable {

  override def execute(scope: Scope): ResultSet = {
    reference match {
      case VariableRef(name) =>
        val variable = scope.lookupVariable(name)
          .getOrElse(throw new IllegalArgumentException(s"No such $getEntityName '$name'"))
        ResultSet(rows = Iterator(Seq(name -> variable.set(scope, value).orNull)))
      case unhandled =>
        throw new IllegalArgumentException(s"Unhandled reference type '${unhandled.getName}'")
    }
  }

  private def getEntityName = reference match {
    case ColumnRef(_) => "field"
    case VariableRef(_) => "variable"
    case unhandled =>
      throw new IllegalArgumentException(s"Unhandled reference type '${unhandled.getName}'")
  }

}
