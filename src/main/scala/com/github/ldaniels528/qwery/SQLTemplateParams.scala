package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops._

/**
  * Represents the extracted SQL template properties
  * @param atoms         the named collection of identifiers (e.g. "FROM './customers.csv')
  * @param conditions    the named collection of conditions (e.g. "lastTradeDate >= now() + 1")
  * @param expressions   the named collection of field/value expression arguments (e.g. "SELECT 1+(2*3), 'Hello', 123, symbol")
  * @param fields        the named collection of field references (e.g. "INSERT INTO (symbol, exchange, lastSale)")
  * @param orderedFields the named collection of ordered fields (e.g. "ORDER BY symbol")
  * @param repeatedSets  the named collection of repeated sequences (e.g. "VALUES ('123', '456') VALUES ('789', '012')")
  */
case class SQLTemplateParams(atoms: Map[String, String] = Map.empty,
                             conditions: Map[String, Condition] = Map.empty,
                             expressions: Map[String, List[Expression]] = Map.empty,
                             fields: Map[String, List[Field]] = Map.empty,
                             orderedFields: Map[String, List[OrderedColumn]] = Map.empty,
                             repeatedSets: Map[String, List[SQLTemplateParams]] = Map.empty) {

  def +(that: SQLTemplateParams): SQLTemplateParams = {
    this.copy(
      atoms = this.atoms ++ that.atoms,
      conditions = this.conditions ++ that.conditions,
      expressions = this.expressions ++ that.expressions,
      fields = this.fields ++ that.fields,
      orderedFields = this.orderedFields ++ that.orderedFields,
      repeatedSets = this.repeatedSets ++ that.repeatedSets)
  }

  /**
    * Indicates whether all of the template mappings are empty
    * @return true, if all of the template mappings are empty
    */
  def isEmpty: Boolean = !nonEmpty

  /**
    * Indicates whether at least one of the template mappings is not empty
    * @return true, if at least one of the template mappings is not empty
    */
  def nonEmpty: Boolean = Seq(atoms, conditions, expressions, fields, orderedFields, repeatedSets).exists(_.nonEmpty)

}