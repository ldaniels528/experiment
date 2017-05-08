package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.sources._

import scala.util.{Failure, Success, Try}

/**
  * Qwery Compiler
  * @author lawrence.daniels@gmail.com
  */
class QweryCompiler() {

  /**
    * Compiles the given query (or statement) into an executable
    * @param query the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def apply(query: String): Executable = compile(query)

  /**
    * Compiles the given query (or statement) into an executable
    * @param query the given query string (e.g. "SELECT * FROM './companylist.csv'")
    * @return an [[Executable executable]]
    */
  def compile(query: String): Executable = {
    TokenStream(query) match {
      case ts if ts.is("INSERT") => parseInsert(ts)
      case ts if ts.is("SELECT") => parseSelect(ts)
      case ts =>
        throw new SyntaxException("Unexpected end of line", ts.peek.orNull)
    }
  }

  /**
    * Parses an INSERT statement
    * @example
    * {{{
    * INSERT INTO './tickers.csv' (symbol, exchange, lastSale)
    * SELECT symbol, exchange, lastSale FROM './EOD-20170505.txt' WHERE exchange = 'NASDAQ'
    * }}}
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Insert executable]]
    */
  private def parseInsert(stream: TokenStream): Insert = {
    implicit val parser = TemplateParser(stream)
    val params = parser.extract("INSERT INTO @target ( @(fields) )")
    val target: Option[QueryOutputSource] = params.identifiers.get("target").flatMap(DataSourceFactory.getOutputSource)
    val fields = params.fieldReferences.getOrElse("fields", throw new SyntaxException("Field arguments missing"))
    val source = stream match {
      case ts if ts.is("SELECT") => parseSelect(ts)
      case ts if ts.is("VALUES") => parseValues(fields, ts)
      case ts =>
        throw new SyntaxException("Invalid query source", ts.peek.orNull)
    }
    Insert(target.getOrElse(throw new SyntaxException("Output source is missing")), fields, source)
  }

  /**
    * Parses a SELECT query
    * @example
    * {{{
    * SELECT symbol, exchange, lastSale FROM './EOD-20170505.txt'
    * WHERE exchange = 'NASDAQ'
    * }}}
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Select executable]]
    */
  private def parseSelect(stream: TokenStream): Select = {
    val parser = TemplateParser(stream)
    val params = parser.extract("SELECT @{fields} FROM @source ?WHERE @<condition> ?GROUP +?BY @(groupBy) ?ORDER +?BY @|orderBy| ?LIMIT @limit")
    Select(
      fields = params.fieldArguments.getOrElse("fields", throw new SyntaxException("Field arguments missing")),
      source = params.identifiers.get("source").flatMap(DataSourceFactory.getInputSource),
      condition = params.expressions.get("condition"),
      groupFields = params.fieldReferences.get("groupBy"),
      sortFields = params.sortFields.get("orderBy"),
      limit = params.identifiers.get("limit").map(parseLimit))
  }

  /**
    * Parses a VALUES clause
    * @param fields the corresponding fields
    * @param ts     the [[TokenStream token stream]]
    * @param parser the implicit [[TemplateParser template parser]]
    * @return the resulting [[Modifications modifications]]
    */
  private def parseValues(fields: Seq[Field], ts: TokenStream)(implicit parser: TemplateParser): Modifications = {
    var valueSets: List[Seq[Any]] = Nil
    while (ts.hasNext) {
      val params = parser.extract("VALUES ( @[values] )")
      params.insertValues.get("values") foreach { values =>
        valueSets = values :: valueSets
      }
    }
    Modifications(fields, valueSets)
  }

  /**
    * Parses the given text value into an integer
    * @param value the given text value
    * @return an integer value
    */
  private def parseLimit(value: String): Int = {
    Try(value.toInt) match {
      case Success(limit) => limit
      case Failure(e) =>
        throw new SyntaxException("Numeric value expected for LIMIT", cause = e)
    }
  }

}
