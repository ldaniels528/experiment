package com.qwery.language

import java.io.{File, InputStream}
import java.net.URL

import com.qwery.models.StorageFormats.StorageFormat
import com.qwery.models.expressions.VariableRef
import com.qwery.models.{StorageFormats, _}
import com.qwery.util.OptionHelper._

import scala.io.Source

/**
  * SQL Language Parser
  * @author lawrence.daniels@gmail.com
  */
trait SQLLanguageParser {

  /**
    * Returns an iterator of executables
    * @param ts the given [[TokenStream token stream]]
    * @return an iterator of [[Invokable]]s
    */
  def iterate(ts: TokenStream): Iterator[Invokable] = new Iterator[Invokable] {
    override def hasNext: Boolean = {
      while (ts.hasNext && (ts is ";")) ts.next()
      ts.hasNext
    }

    override def next(): Invokable = parseNext(ts)
  }

  /**
    * Parses the contents of the given file into an [[Invokable invokable]]
    * @param file the given [[File file]]
    * @return the resultant [[Invokable]]
    */
  def parse(file: File): Invokable = parse(Source.fromFile(file).mkString)

  /**
    * Parses the contents of the given input stream into an [[Invokable invokable]]
    * @param stream the given [[InputStream input stream]]
    * @return the resultant [[Invokable]]
    */
  def parse(stream: InputStream): Invokable = parse(Source.fromInputStream(stream).mkString)

  /**
    * Parses the contents of the given URL into an [[Invokable invokable]]
    * @param url the given [[URL]]
    * @return the resultant [[Invokable]]
    */
  def parse(url: URL): Invokable = parse(Source.fromURL(url).mkString)

  /**
    * Parses the contents of the given string into an [[Invokable invokable]]
    * @param sourceCode the given SQL code string (e.g. "SELECT * FROM Customers")
    * @return the resultant [[Invokable]]
    */
  def parse(sourceCode: String): Invokable = iterate(TokenStream(sourceCode)).toList match {
    case op :: Nil => op
    case ops => SQL(ops: _*)
  }

  /**
    * Parses the next query or statement from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    */
  def parseNext(stream: TokenStream): Invokable = {
    stream match {
      case ts if ts nextIf "(" =>
        val result = parseNext(stream)
        ts.expect(")")
        result
      case ts =>
        ts.decode(tuples =
          "{" -> parseCodeBlock,
          "BEGIN" -> parseBeginToEnd,
          "CALL" -> parseCall,
          "CREATE" -> parseCreate,
          "DEBUG" -> parseConsoleDebug,
          "ERROR" -> parseConsoleError,
          "INCLUDE" -> parseInclude,
          "INFO" -> parseConsoleInfo,
          "INSERT" -> parseInsert,
          "LOG" -> parseConsoleLog,
          "MAIN" -> parseMainProgram,
          "PRINT" -> parseConsolePrint,
          "RETURN" -> parseReturn,
          "SELECT" -> parseSelect,
          "SET" -> parseAssignment,
          "SHOW" -> parseShow,
          "UPDATE" -> parseUpdate,
          "WARN" -> parseConsoleWarn)
    }
  }

  /**
    * Parses the next query (selection), table or variable
    * @param stream the given [[TokenStream]]
    * @return the resultant [[Select]], [[TableRef]] or [[VariableRef]]
    */
  def parseNextQueryTableOrVariable(stream: TokenStream): Invokable = {
    import Aliasable._
    val query = stream match {
      // direct query (e.g. "SELECT * FROM Months")?
      case ts if ts is "SELECT" => parseSelect(ts)
      // indirect query/variable?
      case ts if ts nextIf "(" =>
        val result = parseNext(ts)
        stream expect ")"
        result
      // variable (e.g. "@name")?
      case ts if ts nextIf "@" => VariableRef(ts.next().text)
      // table (e.g. "Months")?
      case ts if ts.isBackticks | ts.isText => TableRef.parse(ts.next().text)
      // unknown
      case ts => ts.die("Query, table or variable expected")
    }

    // is there an alias?
    if (stream nextIf "AS") query.as(alias = stream.next().text) else query
  }

  /**
    * Parses an assignment
    * @example
    * {{{
    *   SET @customers = ( SELECT * FROM Customers WHERE deptId = 31 )
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Assign]]
    */
  protected def parseAssignment(ts: TokenStream): Assign = {
    val params = SQLTemplateParams(ts, "SET %v:variable = %Q:expr")
    Assign(variable = params.variables("variable"), params.sources("expr"))
  }

  /**
    * Parses a BEGIN ... END statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[SQL code block]]
    */
  protected def parseBeginToEnd(ts: TokenStream): SQL = parseSequence(ts, startElem = "BEGIN", endElem = "END")

  /**
    * Parses a CALL statement
    * @example {{{ CALL testInserts('Oil/Gas Transmission') }}}
    * @param ts the given [[TokenStream token stream]]
    * @return
    */
  protected def parseCall(ts: TokenStream): CallProcedure = {
    val params = SQLTemplateParams(ts, "CALL %a:name ( %E:args )")
    CallProcedure(name = params.atoms("name"), args = params.expressions("args"))
  }

  /**
    * Parses a { ... } block
    * @param ts the given [[TokenStream token stream]]
    * @return an [[SQL code block]]
    */
  protected def parseCodeBlock(ts: TokenStream): SQL = parseSequence(ts, startElem = "{", endElem = "}")

  /**
    * Parses a console DEBUG statement
    * @example {{{ DEBUG 'This is a debug message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Debug]]
    */
  protected def parseConsoleDebug(ts: TokenStream): Console.Debug =
    Console.Debug(text = SQLTemplateParams(ts, "DEBUG %a:text").atoms("text"))

  /**
    * Parses a console ERROR statement
    * @example {{{ ERROR 'This is an error message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Error]]
    */
  protected def parseConsoleError(ts: TokenStream): Console.Error =
    Console.Error(text = SQLTemplateParams(ts, "ERROR %a:text").atoms("text"))

  /**
    * Parses a console INFO statement
    * @example {{{ INFO 'This is an informational message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Info]]
    */
  protected def parseConsoleInfo(ts: TokenStream): Console.Info =
    Console.Info(text = SQLTemplateParams(ts, "INFO %a:text").atoms("text"))

  /**
    * Parses a console LOG statement
    * @example {{{ LOG 'This is a log message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Info]]
    */
  protected def parseConsoleLog(ts: TokenStream): Console.Info =
    Console.Info(text = SQLTemplateParams(ts, "LOG %a:text").atoms("text"))

  /**
    * Parses a console PRINT statement
    * @example {{{ PRINT 'This message will be printed to STDOUT' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Print]]
    */
  protected def parseConsolePrint(ts: TokenStream): Console.Print =
    Console.Print(text = SQLTemplateParams(ts, "PRINT %a:text").atoms("text"))

  /**
    * Parses a console WARN statement
    * @example {{{ WARN 'This is a warning message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Warn]]
    */
  protected def parseConsoleWarn(ts: TokenStream): Console.Warn =
    Console.Warn(text = SQLTemplateParams(ts, "WARN %a:text").atoms("text"))

  /**
    * Parses a CREATE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    */
  protected def parseCreate(ts: TokenStream): Invokable = ts.decode(tuples =
    "CREATE EXTERNAL TABLE" -> parseCreateTable,
    "CREATE FUNCTION" -> parseCreateFunction,
    "CREATE LOGICAL TABLE" -> parseCreateLogicTable,
    "CREATE PROCEDURE" -> parseCreateProcedure,
    "CREATE TABLE" -> parseCreateTable,
    "CREATE TEMPORARY FUNCTION" -> parseCreateFunction,
    "CREATE TEMPORARY PROCEDURE" -> parseCreateProcedure,
    "CREATE TEMPORARY VIEW" -> parseCreateView,
    "CREATE VIEW" -> parseCreateView
  )

  /**
    * Parses a CREATE [TEMPORARY] FUNCTION statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Create executable]]
    */
  protected def parseCreateFunction(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE ?TEMPORARY FUNCTION %a:name AS %a:class ?USING +?JAR +?%a:jar")
    Create(UserDefinedFunction(name = params.atoms("name"), `class` = params.atoms("class"), jar = params.atoms.get("jar")))
  }

  /**
    * Parses a CREATE PROCEDURE statement
    * @param ts the [[TokenStream token stream]]
    * @return the resulting [[Create]]
    */
  protected def parseCreateProcedure(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE ?TEMPORARY PROCEDURE %a:name ( ?%P:params ) ?AS %N:code")
    Create(Procedure(name = params.atoms("name"), params = params.columns("params"), code = params.sources("code")))
  }

  /**
    * Parses a CREATE [EXTERNAL] TABLE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Create executable]]
    */
  protected def parseCreateTable(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts,
      """|CREATE ?EXTERNAL TABLE %t:name ( %P:columns )
         |?PARTITIONED +?BY +?( +?%P:partitions +?)
         |?ROW +?FORMAT +?DELIMITED
         |?FIELDS +?TERMINATED +?BY +?%a:delimiter
         |?STORED +?AS +?INPUTFORMAT +?%a:inputFormat
         |?OUTPUTFORMAT +?%a:outputFormat
         |?LOCATION +?%a:path
         |""".stripMargin)
    Create(Table(
      name = params.atoms("name"),
      columns = params.columns.getOrElse("columns", Nil),
      fieldDelimiter = params.atoms.get("delimiter"),
      inputFormat = determineStorageFormat(params.atoms("inputFormat")),
      outputFormat = determineStorageFormat(params.atoms("outputFormat")),
      location = params.atoms.getOrElse("path", ts.die("No location specified"))
    ))
  }

  /**
    * Parses a CREATE LOGICAL TABLE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Create executable]]
    */
  protected def parseCreateLogicTable(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE LOGICAL TABLE %t:name ( %P:columns ) FROM %V:source")
    Create(LogicalTable(
      name = params.atoms("name"),
      columns = params.columns.getOrElse("columns", Nil),
      source = params.sources.getOrElse("source", ts.die("No source specified"))
    ))
  }

  /**
    * Parses a CREATE VIEW statement
    * @example
    * {{{
    * CREATE VIEW OilAndGas AS
    * SELECT Symbol, Name, Sector, Industry, `Summary Quote`
    * FROM Customers
    * WHERE Industry = 'Oil/Gas Transmission'
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[View executable]]
    */
  protected def parseCreateView(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE ?TEMPORARY VIEW %t:name ?AS %Q:query")
    Create(View(name = params.atoms("name"), query = params.sources("query")))
  }

  /**
    * Parses an INCLUDE statement
    * @example
    * {{{
    *   INCLUDE './models.sql'
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Include]]
    */
  protected def parseInclude(ts: TokenStream): Include = {
    val params = SQLTemplateParams(ts, "INCLUDE %a:path")
    Include(paths = List(params.atoms("path")))
  }

  /**
    * Parses an INSERT statement
    * @example
    * {{{
    * INSERT INTO TABLE Tickers (symbol, exchange, lastSale)
    * VALUES ('AAPL', 'NASDAQ', 145.67), ('AMD', 'NYSE', 5.66)
    * }}}
    * @example
    * {{{
    * INSERT OVERWRITE LOCATION './data/companies/service' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    * SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    * FROM Companies WHERE Industry = 'EDP Services'
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Insert]]
    */
  protected def parseInsert(ts: TokenStream): Insert = {
    val params = SQLTemplateParams(ts, "INSERT %C(mode|INTO|OVERWRITE) %L:target ?( +?%F:fields +?) %V:source")
    val fields = params.fields.getOrElse("fields", Nil)
    val isOverwrite = params.atoms.get("mode").contains("OVERWRITE")
    val location = params.locations("target")
    Insert(
      destination = if (isOverwrite) Insert.Overwrite(location) else Insert.Into(location),
      source = params.sources("source"),
      fields = fields)
  }

  /**
    * Parse a MAIN PROGRAM clause - serves as the application entry-point
    * @example
    * {{{
    * MAIN PROGRAM 'StockIngest'
    *   WITH ARGUMENTS AS @args
    *   WITH ENVIRONMENT AS @env
    *   WITH BATCH PROCESSING
    *   WITH HIVE SUPPORT
    * AS
    * BEGIN
    *   INSERT OVERWRITE LOCATION './data/companies/service' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    *   SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    *   FROM Companies WHERE Industry = 'EDP Services'
    * END
    * }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[MainProgram]]
    */
  protected def parseMainProgram(ts: TokenStream): MainProgram = {
    val params = SQLTemplateParams(ts, "MAIN PROGRAM %a:name %W:props ?AS %N:code")
    MainProgram(
      name = params.atoms("name"),
      code = params.sources("code"),
      hiveSupport = params.atoms.get("hiveSupport").nonEmpty,
      streaming = params.atoms.get("processing").map(_.toLowerCase).contains("stream")
    )
  }

  /**
    * Parses a RETURN statement
    * @param ts the [[TokenStream token stream]]
    * @return the resulting [[Return]]
    */
  protected def parseReturn(ts: TokenStream): Return =
    Return(value = SQLTemplateParams(ts, "RETURN ?%q:value").sources.get("value"))

  /**
    * Parses a SELECT query
    * @example
    * {{{
    * SELECT symbol, exchange, lastSale FROM './EOD-20170505.txt' WHERE exchange = 'NASDAQ' LIMIT 5
    * }}}
    * @example
    * {{{
    * SELECT A.symbol, A.exchange, A.lastSale AS lastSaleA, B.lastSale AS lastSaleB
    * FROM 'companlist.csv' AS A
    * INNER JOIN 'companlist2.csv' AS B ON B.Symbol = A.Symbol
    * WHERE A.exchange = 'NASDAQ'
    * LIMIT 5
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Invokable executable]]
    */
  protected def parseSelect(ts: TokenStream): Invokable = {
    val params = SQLTemplateParams(ts,
      """|SELECT ?TOP +?%n:top %E:fields
         |?FROM +?%q:source %J:joins
         |?WHERE +?%c:condition
         |?GROUP +?BY +?%F:groupBy
         |?ORDER +?BY +?%o:orderBy
         |?LIMIT +?%n:limit""".stripMargin)

    // create the SELECT statement
    var select: Invokable = Select(
      fields = params.expressions("fields"),
      from = params.sources.get("source"),
      joins = params.joins.getOrElse("joins", Nil),
      where = params.conditions.get("condition"),
      groupBy = params.fields.getOrElse("groupBy", Nil).map(_.name),
      orderBy = params.orderedFields.getOrElse("orderBy", Nil),
      limit = (params.numerics.get("limit") ?? params.numerics.get("top")).map(_.toInt))

    // is it a UNION statement?
    while (ts nextIf "UNION") {
      select = Union(select, SQLTemplateParams(ts, "%Q:union").sources("union"))
    }
    select
  }

  /**
    * Parses a textual sequence
    * @param ts        the given [[TokenStream token stream]]
    * @param startElem the given starting element (e.g. "{")
    * @param endElem   the given ending element (e.g. "}")
    * @return an [[SQL code block]]
    */
  protected def parseSequence(ts: TokenStream, startElem: String, endElem: String): SQL = {
    var operations: List[Invokable] = Nil
    if (ts nextIf startElem) {
      while (ts.hasNext && !(ts nextIf endElem)) {
        operations = parseNext(ts) :: operations
        if (ts isnt endElem) ts.expect(";")
      }
    }
    SQL(operations.reverse)
  }

  /**
    * Parses a SHOW statement
    * @example {{{ SHOW @results }}}
    * @example {{{ SHOW @results LIMIT 25 }}}
    * @example {{{ SHOW (SELECT * FROM Customers) }}}
    * @example {{{ SHOW (SELECT * FROM Customers) LIMIT 25 }}}
    * @param ts the [[TokenStream token stream]]
    * @return a [[Show]]
    */
  protected def parseShow(ts: TokenStream): Show = {
    val params = SQLTemplateParams(ts, "SHOW %V:rows ?LIMIT +?%n:limit")
    Show(rows = params.sources("rows"), limit = params.numerics.get("limit").map(_.toInt))
  }

  /**
    * Parses an UPDATE statement
    * @example
    * {{{
    * UPDATE Companies
    * SET Symbol = 'AAPL', Name = 'Apple, Inc.', Sector = 'Technology', Industry = 'Computers', LastSale = 203.45
    * WHERE Symbol = 'AAPL'
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Update]]
    */
  protected def parseUpdate(ts: TokenStream): Update = {
    val params = SQLTemplateParams(ts, "UPDATE %L:target SET %U:assignments ?WHERE +?%c:condition")
    Update(
      table = params.locations("target"),
      assignments = params.keyValuePairs("assignments"),
      where = params.conditions.get("condition"))
  }

  private def determineStorageFormat(formatString: String): StorageFormat = formatString.toUpperCase() match {
    case s if s.contains("AVRO") => StorageFormats.AVRO
    case s if s.contains("CSV") => StorageFormats.CSV
    case s if s.contains("JDBC") => StorageFormats.JDBC
    case s if s.contains("JSON") => StorageFormats.JSON
    case s if s.contains("PARQUET") => StorageFormats.PARQUET
    case s if s.contains("ORC") => StorageFormats.ORC
    case _ => throw new IllegalArgumentException(s"Could not determine storage format from '$formatString'")
  }

}
