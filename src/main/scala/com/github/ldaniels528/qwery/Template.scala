package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.{Value, Expression, Field}

/**
  * Represents the extracted template properties
  * @param expressions     the named list of expressions (e.g. "now() + 1")
  * @param fieldArguments  the named list of field/value arguments (e.g. 'Hello', 123, symbol)
  * @param fieldReferences the named list of field references (e.g. "SELECT symbol, exchange, lastSale")
  * @param identifiers     the named list of identifiers (e.g. "FROM './customers.csv')
  * @param insertValues    the named list of value arguments (e.g. "VALUES ('ABC', 123, now() + 1)")
  * @param sortFields      the named list of sort fields (e.g. "ORDER BY symbol")
  */
case class Template(expressions: Map[String, Expression] = Map.empty,
                    fieldArguments: Map[String, List[Value]] = Map.empty,
                    fieldReferences: Map[String, List[Field]] = Map.empty,
                    identifiers: Map[String, String] = Map.empty,
                    insertValues: Map[String, List[Any]] = Map.empty,
                    sortFields: Map[String, List[(Field, Int)]] = Map.empty) {

  def +(that: Template): Template = {
    this.copy(
      fieldArguments = this.fieldArguments ++ that.fieldArguments,
      expressions = this.expressions ++ that.expressions,
      fieldReferences = this.fieldReferences ++ that.fieldReferences,
      identifiers = this.identifiers ++ that.identifiers,
      insertValues = this.insertValues ++ that.insertValues,
      sortFields = this.sortFields ++ that.sortFields)
  }

}