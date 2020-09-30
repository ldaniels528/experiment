package com.qwery.language

import com.qwery.models.ColumnTypes.ColumnType
import com.qwery.models._
import com.qwery.models.expressions._
import com.qwery.util.StringHelper._

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
  * @param locations     the named collection of [[Location locations]]
  * @param numerics      the named collection of numeric values (e.g. "TOP 100")
  * @param orderedFields the named collection of ordered fields (e.g. "ORDER BY symbol")
  * @param repeatedSets  the named collection of repeated sequences (e.g. "VALUES ('123', '456') VALUES ('789', '012')")
  * @param sources       the named collection of queries
  * @param types         the named collection of [[ColumnType types]]
  * @param variables     the named collection of [[VariableRef variables]]
  */
case class SQLTemplateParams(assignables: Map[String, Expression] = Map.empty,
                             atoms: Map[String, String] = Map.empty,
                             columns: Map[String, List[Column]] = Map.empty,
                             conditions: Map[String, Condition] = Map.empty,
                             expressions: Map[String, List[Expression]] = Map.empty,
                             fields: Map[String, List[Field]] = Map.empty,
                             joins: Map[String, List[Join]] = Map.empty,
                             keyValuePairs: Map[String, List[(String, Expression)]] = Map.empty,
                             keywords: Set[String] = Set.empty,
                             locations: Map[String, Location] = Map.empty,
                             numerics: Map[String, Double] = Map.empty,
                             orderedFields: Map[String, List[OrderColumn]] = Map.empty,
                             properties: Map[String, Map[String, String]] = Map.empty,
                             repeatedSets: Map[String, List[SQLTemplateParams]] = Map.empty,
                             sources: Map[String, Invokable] = Map.empty,
                             types: Map[String, (ColumnType, List[Int])] = Map.empty,
                             variables: Map[String, VariableRef] = Map.empty) {

  def +(that: SQLTemplateParams): SQLTemplateParams = {
    this.copy(
      assignables = this.assignables ++ that.assignables,
      atoms = this.atoms ++ that.atoms,
      columns = this.columns ++ that.columns,
      conditions = this.conditions ++ that.conditions,
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
      types = this.types ++ that.types,
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
  def nonEmpty: Boolean = keywords.nonEmpty || Seq(assignables, atoms, columns, conditions, expressions, fields, joins, keyValuePairs,
    locations, numerics, orderedFields, properties, repeatedSets, sources, variables).exists(_.nonEmpty)

}

/**
  * SQLTemplate Params Companion
  * @author lawrence.daniels@gmail.com
  */
object SQLTemplateParams {

  /**
    * Creates a new SQL template parameters instance prepopulated with the tokens parsed via the given template
    * @param ts       the given [[TokenStream token stream]]
    * @param template the given template (e.g. "MAIN PROGRAM %a:name %W:props ?AS %N:code")
    * @return the [[SQLTemplateParser template parser]]
    */
  def apply(ts: TokenStream, template: String): SQLTemplateParams = new SQLTemplateParser(ts).process(cleanup(template))

  /**
    * Removes non-printable characters and extraneous spaces
    * @param s the given string
    * @return the cleaned string
    */
  private def cleanup(s: String): String = {
    val sb = new StringBuilder(s.map { case '\n' | '\r' | '\t' => ' '; case c => c }.trim)
    while (sb.indexOfOpt("  ").map(index => sb.replace(index, index + 2, " ")).nonEmpty) {}
    sb.toString()
  }

  final implicit class MappedParameters[T](val mapped: Map[String, T]) extends AnyVal {
    def is(name: String, f: T => Boolean): Boolean = mapped.get(name).exists(f)
  }

}
