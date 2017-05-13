package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.{Condition, Expression, Field}

/**
  * Represents the extracted SQL template properties
  * @param conditions  the named collection of conditions (e.g. "lastTradeDate >= now() + 1")
  * @param expressions the named collection of field/value expression arguments (e.g. "SELECT 1+(2*3), 'Hello', 123, symbol")
  * @param fields      the named collection of field references (e.g. "INSERT INTO (symbol, exchange, lastSale)")
  * @param identifiers the named collection of identifiers (e.g. "FROM './customers.csv')
  * @param sortFields  the named collection of sort fields (e.g. "ORDER BY symbol")
  */
case class SQLTemplateParams(conditions: Map[String, Condition] = Map.empty,
                             expressions: Map[String, List[Expression]] = Map.empty,
                             fields: Map[String, List[Field]] = Map.empty,
                             identifiers: Map[String, String] = Map.empty,
                             sortFields: Map[String, List[(Field, Int)]] = Map.empty) {

  def +(that: SQLTemplateParams): SQLTemplateParams = {
    this.copy(
      conditions = this.conditions ++ that.conditions,
      expressions = this.expressions ++ that.expressions,
      fields = this.fields ++ that.fields,
      identifiers = this.identifiers ++ that.identifiers,
      sortFields = this.sortFields ++ that.sortFields)
  }

}