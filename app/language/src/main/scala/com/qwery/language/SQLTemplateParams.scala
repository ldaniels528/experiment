package com.qwery.language

import com.qwery.models._
import com.qwery.models.expressions._

/**
  * Represents the extracted SQL template properties
  * @param atoms         the named collection of identifiers (e.g. "FROM './customers.csv')
  * @param columns       the named collection of columns
  * @param conditions    the named collection of conditions (e.g. "lastTradeDate >= now() + 1")
  * @param assignables   the named collection of singular expressions (e.g. "SET X = 1+(2*x)")
  * @param expressions   the named collection of key-value pair assignments (e.g. "X" -> "1+(2*x)")
  * @param fields        the named collection of field references (e.g. "INSERT INTO (symbol, exchange, lastSale)")
  * @param joins         the named collection of join references (e.g. "INNER JOIN './stocks.csv' ON A.symbol = B.symbol")
  * @param keyValuePairs the named collection of key-value pairs (e.g. "key = 'Hello', value = 123")
  * @param keywords      the named collection of key words
  * @param numerics      the named collection of numeric values (e.g. "TOP 100")
  * @param orderedFields the named collection of ordered fields (e.g. "ORDER BY symbol")
  * @param repeatedSets  the named collection of repeated sequences (e.g. "VALUES ('123', '456') VALUES ('789', '012')")
  * @param sources       the named collection of queries
  * @param variables     the named collection of variables
  */
case class SQLTemplateParams(atoms: Map[String, String] = Map.empty,
                             columns: Map[String, List[Column]] = Map.empty,
                             conditions: Map[String, Condition] = Map.empty,
                             assignables: Map[String, Expression] = Map.empty,
                             expressions: Map[String, List[Expression]] = Map.empty,
                             fields: Map[String, List[Field]] = Map.empty,
                             joins: Map[String, List[Join]] = Map.empty,
                             locations: Map[String, Location] = Map.empty,
                             keyValuePairs: Map[String, List[(String, Expression)]] = Map.empty,
                             keywords: Set[String] = Set.empty,
                             numerics: Map[String, Double] = Map.empty,
                             orderedFields: Map[String, List[OrderColumn]] = Map.empty,
                             properties: Map[String, Map[String, String]] = Map.empty,
                             repeatedSets: Map[String, List[SQLTemplateParams]] = Map.empty,
                             sources: Map[String, Invokable] = Map.empty,
                             variables: Map[String, VariableRef] = Map.empty) {

  def +(that: SQLTemplateParams): SQLTemplateParams = {
    this.copy(
      atoms = this.atoms ++ that.atoms,
      columns = this.columns ++ that.columns,
      conditions = this.conditions ++ that.conditions,
      assignables = this.assignables ++ that.assignables,
      expressions = this.expressions ++ that.expressions,
      fields = this.fields ++ that.fields,
      joins = this.joins ++ that.joins,
      keyValuePairs = this.keyValuePairs ++ that.keyValuePairs,
      keywords = this.keywords ++ that.keywords,
      locations = this.locations ++ that.locations,
      numerics = this.numerics ++ that.numerics,
      orderedFields = this.orderedFields ++ that.orderedFields,
      properties = this.properties ++ that.properties,
      repeatedSets = this.repeatedSets ++ that.repeatedSets,
      sources = this.sources ++ that.sources,
      variables = this.variables ++ that.variables)
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
  def nonEmpty: Boolean = Seq(atoms, columns, conditions, assignables, expressions, fields, joins, keyValuePairs,
    keywords, numerics, orderedFields, locations, repeatedSets, sources, variables).exists(_.nonEmpty)

}

/**
  * SQLTemplate Params Companion
  * @author lawrence.daniels@gmail.com
  */
object SQLTemplateParams {

  /**
    * Creates a new SQL Language Parser instance
    * @param ts the given [[TokenStream token stream]]
    * @return the [[SQLTemplateParser template parser]]
    */
  def apply(ts: TokenStream, template: String): SQLTemplateParams = new SQLTemplateParser(ts).process(cleanup(template))

  private def cleanup(text: String): String = {
    var s = text
      .replaceAllLiterally("\t", " ")
      .replaceAllLiterally("\n", " ")
      .replaceAllLiterally("\r", " ")
      .trim
    while (s.contains("  ")) {
      s = s.replaceAllLiterally("  ", " ")
    }
    s
  }

}
