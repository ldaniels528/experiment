package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops._

/**
  * Represents the extracted SQL template properties
  * @param atoms         the named collection of identifiers (e.g. "FROM './customers.csv')
  * @param conditions    the named collection of conditions (e.g. "lastTradeDate >= now() + 1")
  * @param expressions   the named collection of field/value expression arguments (e.g. "SELECT 1+(2*3), 'Hello', 123, symbol")
  * @param fields        the named collection of field references (e.g. "INSERT INTO (symbol, exchange, lastSale)")
  * @param orderedFields the named collection of sort fields (e.g. "ORDER BY symbol")
  */
case class SQLTemplateParams(atoms: Map[String, String] = Map.empty,
                             conditions: Map[String, Condition] = Map.empty,
                             expressions: Map[String, List[Expression]] = Map.empty,
                             fields: Map[String, List[Field]] = Map.empty,
                             orderedFields: Map[String, List[OrderedColumn]] = Map.empty) {

  def +(that: SQLTemplateParams): SQLTemplateParams = {
    this.copy(
      conditions = this.conditions ++ that.conditions,
      expressions = this.expressions ++ that.expressions,
      fields = this.fields ++ that.fields,
      atoms = this.atoms ++ that.atoms,
      orderedFields = this.orderedFields ++ that.orderedFields)
  }

}