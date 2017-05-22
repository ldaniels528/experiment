package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.sources._
import com.github.ldaniels528.qwery.util.OptionHelper._
import com.github.ldaniels528.qwery.util.StringHelper._

import scala.util.{Failure, Success, Try}

/**
  * Qwery Compiler
  * @author lawrence.daniels@gmail.com
  */
class QweryCompiler {

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
  def compile(query: String): Executable = parseNext(TokenStream(query))

  /**
    * Parses the next query or statement from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Executable]]
    */
  def parseNext(stream: TokenStream): Executable = {
    stream match {
      case ts if ts is "DESCRIBE" => parseDescribe(ts)
      case ts if ts is "INSERT" => parseInsert(ts)
      case ts if ts is "SELECT" => parseSelect(ts)
      case ts => die("Unexpected end of line", ts)
    }
  }

  private def die[A](message: String, ts: TokenStream): A = {
    throw new SyntaxException(message, ts.peek.orNull)
  }

  /**
    * Parses a DESCRIBE statement
    * @example {{{ DESCRIBE './companylist.csv' }}}
    * @example {{{ DESCRIBE './companylist.csv' LIMIT 5 }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Describe executable]]
    */
  private def parseDescribe(ts: TokenStream): Describe = {
    val params = SQLTemplateParser(ts).process("DESCRIBE @source ?LIMIT ?@limit")
    Describe(
      source = params.atoms.get("source").map(DataResource.apply(_)).getOrElse(die("No source provided", ts)),
      limit = params.atoms.get("limit").map(parseInteger(_, "Numeric value expected for LIMIT")))
  }

  /**
    * Parses an INSERT statement
    * @example
    * {{{
    * INSERT INTO './tickers.csv' (symbol, exchange, lastSale)
    * SELECT symbol, exchange, lastSale FROM './EOD-20170505.txt' WHERE exchange = 'NASDAQ'
    * }}}
    * @example
    * {{{
    * INSERT OVERWRITE './companyinfo.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    * SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    * FROM './companylist.csv' WHERE Industry = 'EDP Services'
    * }}}
    * @example
    * {{{
    * INSERT INTO 'companylist.json' (Symbol, Name, Sector, Industry) WITH FORMAT JSON
    * SELECT Symbol, Name, Sector, Industry, `Summary Quote`
    * FROM 'companylist.csv' WITH FORMAT CSV
    * WHERE Industry = 'Oil/Gas Transmission'
    * }}}
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Insert executable]]
    */
  private def parseInsert(stream: TokenStream): Insert = {
    val parser = SQLTemplateParser(stream)
    val params = parser.process(
      """
        |INSERT @|mode|INTO|OVERWRITE| @target ( @(fields) )
        |?WITH ?@|withKey|COLUMN|DELIMITER|FORMAT|QUOTED| ?@withValue""".stripMargin.toSingleLine)
    val hints = processHints(stream, params)
    val target = params.atoms.get("target").map(DataResource.apply(_, hints))
      .getOrElse(throw new SyntaxException("Output source is missing"))
    val fields = params.fields
      .getOrElse("fields", die("Field arguments missing", stream))
    val append = params.atoms.get("mode").exists(_.equalsIgnoreCase("INTO"))
    // VALUES or SELECT
    val source = stream match {
      case ts if ts.is("VALUES") => parseInsertValues(fields, ts, parser)
      case ts => parseNext(ts)
    }
    Insert(target, fields, source, append)
  }

  /**
    * Parses WITH clauses
    * @example WITH ROWS AS CSV|PSV|TSV|JSON
    * @example WITH DELIMITER ','
    * @example WITH QUOTED TEXT
    * @example WITH QUOTED NUMBERS
    * @param stream the given [[TokenStream token stream]]
    */
  private def parseWithClause(stream: TokenStream): Hints = {
    val withDelimiter = "WITH DELIMITER @type"
    val withQuotedNumbers = "WITH QUOTED NUMBERS"
    val withQuotedText = "WITH QUOTED TEXT"
    val withRowsAs = "WITH FORMAT @format"
    val parser = SQLTemplateParser(stream)
    var hints = Hints()
    while (stream.is("WITH")) {
      parser match {
        // WITH DELIMITER ','
        case p if p.matches(withDelimiter) =>
          val params = p.process(withDelimiter)
          val delimiter = params.atoms.getOrElse("type", stream.die("A text delimiter was expected"))
          hints = hints.copy(delimiter = Some(delimiter))
        // WITH QUOTED NUMBERS
        case p if p.matches(withQuotedNumbers) =>
          p.process(withQuotedNumbers)
          hints = hints.copy(quotedNumbers = Some(true))
        // WITH QUOTED TEXT
        case p if p.matches(withQuotedText) =>
          p.process(withQuotedText)
          hints = hints.copy(quotedText = Some(true))
        // WITH ROWS AS CSV|PSV|TSV|JSON
        case p if p.matches(withRowsAs) =>
          val params = p.process(withRowsAs)
          val format = params.atoms.getOrElse("format", stream.die("A format identifier was expected (CSV, JSON, PSV or TSV"))
          hints = hints.usingFormat(format = format)
        case _ =>
          stream.die("Syntax error")
      }
    }
    hints
  }

  /**
    * Parses an INSERT VALUES clause
    * @param fields the corresponding fields
    * @param ts     the [[TokenStream token stream]]
    * @param parser the implicit [[SQLTemplateParser template parser]]
    * @return the resulting [[InsertValues modifications]]
    */
  private def parseInsertValues(fields: Seq[Field], ts: TokenStream, parser: SQLTemplateParser): InsertValues = {
    val valueSets = parser.process("{{valueSet VALUES ( @{values} ) }}").repeatedSets.get("valueSet") match {
      case Some(sets) => sets.flatMap(_.expressions.get("values"))
      case None =>
        throw new SyntaxException("VALUES clause could not be parsed", ts.previous.orNull)
    }
    if (!valueSets.forall(_.size == fields.size))
      throw new SyntaxException("The number of fields must match the number of values", ts.previous.orNull)
    InsertValues(fields, valueSets)
  }

  /**
    * Parses a SELECT query
    * @example
    * {{{
    * SELECT symbol, exchange, lastSale FROM './EOD-20170505.txt'
    * WHERE exchange = 'NASDAQ'
    * LIMIT 5
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Select executable]]
    */
  private def parseSelect(ts: TokenStream): Select = {
    val params = SQLTemplateParser(ts).process(
      """
        |SELECT ?TOP ?@top @{fields}
        |?FROM ?@source
        |?WITH ?@|withKey|COLUMN|DELIMITER|FORMAT|QUOTED| ?@withValue
        |?WHERE ?@!{condition}
        |?GROUP +?BY ?@(groupBy)
        |?ORDER +?BY ?@[orderBy]
        |?LIMIT ?@limit""".stripMargin.toSingleLine)
    Select(
      fields = params.expressions.getOrElse("fields", die("Field arguments missing", ts)),
      source = params.atoms.get("source").map(DataResource.apply(_, processHints(ts, params))),
      condition = params.conditions.get("condition"),
      groupFields = params.fields.getOrElse("groupBy", Nil),
      orderedColumns = params.orderedFields.getOrElse("orderBy", Nil),
      limit = (params.atoms.get("limit") ?? params.atoms.get("top"))
        .map(parseInteger(_, "Numeric value expected LIMIT or TOP"))
    )
  }

  private def processHints(ts: TokenStream, params: SQLTemplateParams): Option[Hints] = {
    import com.github.ldaniels528.qwery.util.OptionHelper.Risky._
    for {
      key <- params.atoms.get("withKey")
      value <- params.atoms.get("withValue")
    } yield {
      key match {
        case "COLUMN" => Hints(headers = value.equalsIgnoreCase("HEADERS"))
        case "DELIMITER" => Hints(delimiter = value)
        case "FORMAT" => Hints().usingFormat(value)
        case "QUOTED" => Hints(quotedNumbers = value.equalsIgnoreCase("NUNBERS"), quotedText = value.equalsIgnoreCase("TEXT"))
        case _ => ts.die("Invalid verb for WITH")
      }
    }
  }

  /**
    * Parses the given text value into an integer
    * @param value the given text value
    * @return an integer value
    */
  private def parseInteger(value: String, message: String): Int = {
    Try(value.toInt) match {
      case Success(limit) => limit
      case Failure(e) =>
        throw new SyntaxException(message, cause = e)
    }
  }

}

/**
  * Qwery Compiler Singleton
  * @author lawrence.daniels@gmail.com
  */
object QweryCompiler extends QweryCompiler