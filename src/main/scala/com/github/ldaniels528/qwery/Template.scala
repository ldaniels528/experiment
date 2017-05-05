package com.github.ldaniels528.qwery

/**
  * Represents a template
  * @param arguments     the named arguments
  * @param fields the named argument lists
  * @param expressions   the named expressions
  */
case class Template(arguments: Map[String, String] = Map.empty,
                    fields: Map[String, List[Field]] = Map.empty,
                    expressions: Map[String, Expression] = Map.empty,
                    values: Map[String, List[Any]] = Map.empty) {

  def +(that: Template): Template = {
    this.copy(
      arguments = this.arguments ++ that.arguments,
      fields = this.fields ++ that.fields,
      expressions = this.expressions ++ that.expressions,
      values = this.values ++ that.values)
  }

}