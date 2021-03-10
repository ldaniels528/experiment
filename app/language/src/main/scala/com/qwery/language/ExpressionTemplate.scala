package com.qwery.language

import com.qwery.models.ColumnTypeSpec
import com.qwery.models.expressions.{Condition, Expression, FieldRef, VariableRef}

/**
  * Expression Template
  * @author lawrence.daniels@gmail.com
  */
case class ExpressionTemplate(atoms: Map[String, String] = Map.empty,
                              conditions: Map[String, Condition] = Map.empty,
                              expressions: Map[String, Expression] = Map.empty,
                              expressionLists: Map[String, List[Expression]] = Map.empty,
                              fields: Map[String, FieldRef] = Map.empty,
                              types: Map[String, ColumnTypeSpec] = Map.empty,
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
