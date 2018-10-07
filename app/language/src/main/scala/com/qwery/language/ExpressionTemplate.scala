package com.qwery.language

import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models.expressions.{Condition, Expression, Field, VariableRef}

/**
  * Expression Template
  * @author lawrence.daniels@gmail.com
  */
case class ExpressionTemplate(atoms: Map[String, String] = Map.empty,
                              conditions: Map[String, Condition] = Map.empty,
                              expressions: Map[String, Expression] = Map.empty,
                              expressionLists: Map[String, List[Expression]] = Map.empty,
                              fields: Map[String, Field] = Map.empty,
                              types: Map[String, ColumnType] = Map.empty,
                              variables: Map[String, VariableRef] = Map.empty) {

  def +(that: ExpressionTemplate): ExpressionTemplate = {
    this.copy(
      atoms = atoms ++ that.atoms,
      conditions = conditions ++ that.conditions,
      expressions = expressions ++ that.expressions,
      expressionLists = expressionLists ++ that.expressionLists,
      fields = fields ++ that.fields,
      types = types ++ that.types,
      variables = variables ++ that.variables
    )
  }

}
