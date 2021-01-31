package com.qwery.language

import java.io.{File, InputStream}
import java.net.URL

import com.qwery.language.SQLTemplateParams.MappedParameters
import com.qwery.models.StorageFormats.StorageFormat
import com.qwery.models.expressions._
import com.qwery.models.{StorageFormats, _}
import com.qwery.util.OptionHelper._
import com.qwery.util.ResourceHelper._

import scala.io.Source
import scala.language.postfixOps

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
      while (ts.hasNext && (ts nextIf ";")) {}
      ts.hasNext
    }

    override def next(): Invokable = parseNext(ts)
  }

  /**
    * Parses the next query or statement from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    */
  def parseNext(stream: TokenStream): Invokable = stream.decode(tuples =
    "(" -> (ts => parseIndirectQuery(ts)(parseNextSubQuery)),
    "{" -> (ts => parseSequence(ts, startElem = "{", endElem = "}")),
    "BEGIN" -> (ts => parseSequence(ts, startElem = "BEGIN", endElem = "END")),
    "CALL" -> parseCall,
    "CREATE" -> parseCreate,
    "DECLARE" -> parseDeclare,
    "DEBUG" -> parseConsoleDebug,
    "DELETE" -> parseDelete,
    "DROP" -> parseDrop,
    "ERROR" -> parseConsoleError,
    "FOR" -> parseForLoop,
    "INCLUDE" -> parseInclude,
    "INFO" -> parseConsoleInfo,
    "INSERT" -> parseInsert,
    "PRINT" -> parseConsolePrint,
    "RETURN" -> parseReturn,
    "SELECT" -> parseSelect,
    "SET" -> parseSet,
    "SHOW" -> parseShow,
    "TRUNCATE" -> parseTruncate,
    "UPDATE" -> parseUpdate,
    "WARN" -> parseConsoleWarn,
    "WHILE" -> parseWhile
  )

  /**
    * Parses a CALL statement
    * @example {{{ CALL testInserts('Oil/Gas Transmission') }}}
    * @param ts the given [[TokenStream token stream]]
    * @return
    */
  def parseCall(ts: TokenStream): ProcedureCall = {
    val params = SQLTemplateParams(ts, "CALL %a:name ( %E:args )")
    ProcedureCall(name = params.atoms("name"), args = params.expressions("args"))
  }

  /**
    * Parses a console DEBUG statement
    * @example {{{ DEBUG 'This is a debug message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Debug]]
    */
  def parseConsoleDebug(ts: TokenStream): Console.Debug =
    Console.Debug(text = SQLTemplateParams(ts, "DEBUG %a:text").atoms("text"))

  /**
    * Parses a console ERROR statement
    * @example {{{ ERROR 'This is an error message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Error]]
    */
  def parseConsoleError(ts: TokenStream): Console.Error =
    Console.Error(text = SQLTemplateParams(ts, "ERROR %a:text").atoms("text"))

  /**
    * Parses a console INFO statement
    * @example {{{ INFO 'This is an informational message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Info]]
    */
  def parseConsoleInfo(ts: TokenStream): Console.Info =
    Console.Info(text = SQLTemplateParams(ts, "INFO %a:text").atoms("text"))

  /**
    * Parses a console PRINT statement
    * @example {{{ PRINT 'This message will be printed to STDOUT' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Print]]
    */
  def parseConsolePrint(ts: TokenStream): Console.Print =
    Console.Print(text = SQLTemplateParams(ts, "PRINT %a:text").atoms("text"))

  /**
    * Parses a console WARN statement
    * @example {{{ WARN 'This is a warning message' }}}
    * @param ts the [[TokenStream token stream]]
    * @return the [[Console.Warn]]
    */
  def parseConsoleWarn(ts: TokenStream): Console.Warn =
    Console.Warn(text = SQLTemplateParams(ts, "WARN %a:text").atoms("text"))

  /**
    * Parses a CREATE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    */
  def parseCreate(ts: TokenStream): Invokable = ts.decode(tuples =
    "CREATE COLUMNAR TABLE" -> parseCreateTable,
    "CREATE EXTERNAL TABLE" -> parseCreateTableExternal,
    "CREATE FUNCTION" -> parseCreateFunction,
    "CREATE INDEX" -> parseCreateTableIndex,
    "CREATE INLINE TABLE" -> parseCreateInlineTable,
    "CREATE PARTITIONED TABLE" -> parseCreateTable,
    "CREATE PROCEDURE" -> parseCreateProcedure,
    "CREATE TABLE" -> parseCreateTable,
    "CREATE TYPE" -> parseCreateTypeAsEnum,
    "CREATE VIEW" -> parseCreateView
  )

  /**
    * Parses a CREATE FUNCTION statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Create executable]]
    */
  def parseCreateFunction(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE FUNCTION ?%IFNE:exists %a:name AS %a:class ?USING +?JAR +?%a:jar")
    Create(UserDefinedFunction(name = params.atoms("name"), `class` = params.atoms("class"), jarLocation = params.atoms.get("jar")))
  }

  /**
    * Parses a CREATE INLINE TABLE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Create executable]]
    */
  def parseCreateInlineTable(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE INLINE TABLE ?%IFNE:exists %t:name ( %P:columns ) FROM %V:source")
    Create(InlineTable(
      name = params.atoms("name"),
      columns = params.columns.getOrElse("columns", Nil),
      source = params.sources.getOrElse("source", ts.die("No source specified"))
    ))
  }

  /**
    * Parses a CREATE PROCEDURE statement
    * @param ts the [[TokenStream token stream]]
    * @return the resulting [[Create]]
    */
  def parseCreateProcedure(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE PROCEDURE ?%IFNE:exists %a:name ( ?%P:params ) ?AS %N:code")
    Create(Procedure(name = params.atoms("name"), params = params.columns("params"), code = params.sources("code")))
  }

  /**
    * Parses a CREATE [COLUMNAR|PARTITIONED] TABLE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Create executable]]
    */
  def parseCreateTable(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE ?%C(mode|COLUMNAR|PARTITIONED) TABLE ?%IFNE:exists %t:name ( %P:columns ) ?%w:props")
    Create(Table(
      name = params.atoms("name"),
      description = params.atoms.get("description"),
      columns = params.columns.getOrElse("columns", Nil),
      ifNotExists = params.indicators.get("exists").contains(true),
      isColumnar = params.atoms.is("mode", _ equalsIgnoreCase "COLUMNAR"),
      isPartitioned = params.atoms.is("mode", _ equalsIgnoreCase "PARTITIONED")
    ))
  }

  /**
    * Parses a CREATE EXTERNAL TABLE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Create executable]]
    */
  def parseCreateTableExternal(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE EXTERNAL TABLE ?%IFNE:exists %t:name ( %P:columns ) ?%w:props")

    def escapeChars(string: String): String = {
      val replacements = Seq("\\n" -> "\n", "\\r" -> "\r", "\\t" -> "\t") // TODO \u0000
      replacements.foldLeft(string) { case (line, (a, b)) => line.replaceAllLiterally(a, b) }
    }

    def getLocation: Option[Location] = {
      if (params.atoms.contains("path")) Option(LocationRef(params.atoms("path")))
      else if (params.variables.contains("path"))
        params.variables("path") match {
          case v: LocalVariableRef => Option(VariableLocationRef(v))
          case _ => ts.die("Only scalar variables can be used")
        }
      else None
    }

    Create(ExternalTable(
      name = params.atoms("name"),
      description = params.atoms.get("description"),
      columns = params.columns.getOrElse("columns", Nil),
      ifNotExists = params.indicators.get("exists").contains(true),
      fieldTerminator = params.atoms.get("field.delimiter").map(escapeChars),
      headersIncluded = params.atoms.get("props.headers").map(_ equalsIgnoreCase "ON"),
      inputFormat = params.atoms.get("formats.input").map(determineStorageFormat),
      lineTerminator = params.atoms.get("line.delimiter").map(escapeChars),
      location = getLocation,
      nullValue = params.atoms.get("props.nullValue"),
      outputFormat = params.atoms.get("formats.output").map(determineStorageFormat),
      partitionBy = params.columns.getOrElse("partitions", Nil),
      serdeProperties = params.properties.getOrElse("props.serde", Map.empty),
      tableProperties = params.properties.getOrElse("props.table", Map.empty)
    ))
  }

  /**
   * Parses a CREATE INDEX statement
   * @param ts the [[TokenStream token stream]]
   * @return an [[Create executable]]
   * @example {{{
   * CREATE INDEX stocks_symbol ON stocks (name)
   * }}}
   */
  def parseCreateTableIndex(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE INDEX ?%IFNE:exists %a:name ON %L:table ( %F:columns )")
    Create(TableIndex(
      name = params.atoms("name"),
      columns = params.fields("columns"),
      table = params.locations("table"),
      ifNotExists = params.indicators.get("exists").contains(true)
    ))
  }

  /**
   * Parses a CREATE TYPE ... AS ENUM statement
   * @param ts the [[TokenStream token stream]]
   * @return an [[Create executable]]
   * @example {{{
   * CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
   * }}}
   */
  def parseCreateTypeAsEnum(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE TYPE ?%IFNE:exists %t:name AS ENUM ( %E:values )")
    Create(TypeAsEnum(name = params.atoms("name"), values = params.expressions("values") map {
      case Literal(value: String) => value
      case other => throw SyntaxException(s"String constant expected near '$other'", ts)
    }))
  }

  /**
    * Parses a CREATE VIEW statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Invokable invokable]]
   * @example
   * {{{
   * CREATE VIEW OilAndGas
   * WITH DESCRIPTION 'Oil & Gas Stocks sorted by symbol'
   * AS
   * SELECT Symbol, Name, Sector, Industry, `Summary Quote`
   * FROM Customers
   * WHERE Industry = 'Oil/Gas Transmission'
   * ORDER BY Symbol DESC
   * }}}
    */
  def parseCreateView(ts: TokenStream): Create = {
    val params = SQLTemplateParams(ts, "CREATE VIEW ?%IFNE:exists %t:name ?%w:props ?AS %Q:query")
    Create(View(
      name = params.atoms("name"),
      query = params.sources("query"),
      description = params.atoms.get("description"),
      ifNotExists = params.indicators.get("exists").contains(true)))
  }

  /**
    * Parses a DECLARE statement
    * @param ts the given [[TokenStream token stream]]
    * @example DECLARE EXTERNAL @firstnames STRING
    * @return an [[Invokable invokable]]
    */
  def parseDeclare(ts: TokenStream): Invokable = {
    val params = SQLTemplateParams(ts, "DECLARE ?%C(mode|EXTERNAL) %v:variable %a:type")
    val `type` = params.atoms("type")
    val isExternal = params.atoms.is("mode", _ equalsIgnoreCase "EXTERNAL")
    Declare(variable = params.variables("variable"), `type` = `type`, isExternal = isExternal)
  }

  def parseDelete(ts: TokenStream): Delete = {
    val params = SQLTemplateParams(ts, "DELETE FROM %t:name ?WHERE +?%c:condition ?LIMIT +?%n:limit")
    Delete(Table(name = params.atoms("name")), where = params.conditions.get("condition"), limit = params.numerics.get("limit").map(_.toInt))
  }

  /**
   * Parses a DROP statement
   * @param ts the given [[TokenStream token stream]]
   * @return an [[Invokable invokable]]
   */
  def parseDrop(ts: TokenStream): Invokable = ts.decode(tuples =
    "DROP TABLE" -> parseDropTable,
    "DROP VIEW" -> parseDropView,
  )

  /**
   * Parses a DROP TABLE statement
   * @param ts the given [[TokenStream token stream]]
   * @return a [[DropTable]]
   */
  def parseDropTable(ts: TokenStream): DropTable = {
    val params = SQLTemplateParams(ts, "DROP TABLE ?%IFE:exists %t:name")
    DropTable(Table(name = params.atoms("name")), ifExists = params.indicators.get("exists").contains(true))
  }

  /**
    * Parses a DROP VIEW statement
    * @param ts the given [[TokenStream token stream]]
    * @return a [[DropView]]
    */
  def parseDropView(ts: TokenStream): DropView = {
    val params = SQLTemplateParams(ts, "DROP VIEW ?%IFE:exists %t:name")
    DropView(Table(name = params.atoms("name")), ifExists = params.indicators.get("exists").contains(true))
  }

  /**
    * Parses a FOR statement
    * @example
    * {{{
    * FOR @item IN (
    *   SELECT symbol, lastSale FROM Securities WHERE naics = '12345'
    * )
    * LOOP
    *   PRINT '${item.symbol}' is ${item.lastSale)/share';
    * END LOOP;
    * }}}
    * @param stream the given [[TokenStream token stream]]
    * @return an [[While]]
    */
  def parseForLoop(stream: TokenStream): ForEach = {
    val params = SQLTemplateParams(stream, "FOR %v:variable IN ?%k:REVERSE %q:rows")
    val variable = params.variables("variable") match {
      case v: RowSetVariableRef => v
      case _ => stream.die("Local variables are not compatible with row sets")
    }
    ForEach(variable, rows = params.sources("rows"),
      invokable = stream match {
        case ts if ts is "LOOP" => parseSequence(ts, startElem = "LOOP", endElem = "END LOOP")
        case ts => parseNext(ts)
      },
      isReverse = params.keywords.exists(_ equalsIgnoreCase "REVERSE"))
  }

  /**
    * Parses an INCLUDE statement
    * @example {{{ INCLUDE './models.sql' }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Include]]
    */
  def parseInclude(ts: TokenStream): Include = {
    Include(path = SQLTemplateParams(ts, "INCLUDE %a:path").atoms("path"))
  }

  /**
    * Parses an indirect query from the stream (e.g. "( SELECT * FROM Customers ) AS C")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    */
  def parseIndirectQuery(ts: TokenStream)(f: TokenStream => Invokable): Invokable = {
    parseNextAlias(ts.extract(begin = "(", end = ")")(f), ts)
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
  def parseInsert(ts: TokenStream): Insert = {
    val params = SQLTemplateParams(ts, "INSERT %C(mode|INTO|OVERWRITE) %L:target ?( +?%F:fields +?) %V:source")
    val fields = params.fields.getOrElse("fields", Nil)
    val isOverwrite = params.atoms.get("mode").exists(_ equalsIgnoreCase "OVERWRITE")
    val location = params.locations("target")
    Insert(
      destination = if (isOverwrite) Insert.Overwrite(location) else Insert.Into(location),
      source = params.sources("source").toQueryable,
      fields = fields)
  }

  /**
    * Optionally parses an alias (e.g. "( ... ) AS O")
    * @param entity the given [[Invokable]]
    * @param ts     the given [[TokenStream]]
    * @return the resultant [[Invokable]]
    */
  def parseNextAlias(entity: Invokable, ts: TokenStream): Invokable = {
    import Aliasable._
    if (ts nextIf "AS") entity.as(alias = ts.next().text) else entity
  }

  /**
    * Parses the next query from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    */
  def parseNextQueryOrVariable(stream: TokenStream): Invokable = stream match {
    // indirect query?
    case ts if ts is "(" => parseIndirectQuery(ts)(parseNextQueryOrVariable)
    // row variable (e.g. "@results")?
    case ts if ts nextIf "#" => parseNextAlias(@#(ts.next().text), ts)
    // field variable (e.g. "$name")?
    case ts if ts nextIf "@" => ts.die("Local variable references are not compatible with row sets")
    // sub-query?
    case ts => parseNextSubQuery(ts)
  }

  /**
    * Parses the next query (selection), table or variable
    * @param stream the given [[TokenStream]]
    * @return the resultant [[Select]], [[TableRef]] or [[VariableRef]]
    */
  def parseNextQueryTableOrVariable(stream: TokenStream): Invokable = stream match {
    // table (e.g. "Months" or "`Months of the Year`")?
    case ts if ts.isBackticks | ts.isDoubleQuoted | ts.isText => parseNextAlias(Table(ts.next().text), ts)
    // must be a sub-query or variable
    case ts => parseNextQueryOrVariable(ts)
  }

  /**
    * Parses the next query from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    */
  def parseNextSubQuery(stream: TokenStream): Invokable = stream.decode(tuples =
    "CALL" -> parseCall,
    "SELECT" -> parseSelect
  )

  /**
    * Parses a RETURN statement
    * @param ts the [[TokenStream token stream]]
    * @return the resulting [[Return]]
    */
  def parseReturn(ts: TokenStream): Return =
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
    * FROM Securities AS A
    * INNER JOIN Securities AS B ON B.symbol = A.symbol
    * WHERE A.exchange = 'NASDAQ'
    * LIMIT 5
    * }}}
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Invokable executable]]
    */
  def parseSelect(stream: TokenStream): Invokable = {
    val params = SQLTemplateParams(stream,
      """|SELECT ?TOP +?%n:top %E:fields
         |?%C(mode|INTO|OVERWRITE) +?%L:target
         |?FROM +?%q:source %J:joins
         |?WHERE +?%c:condition
         |?GROUP +?BY +?%F:groupBy
         |?HAVING +?%c:havingCondition
         |?ORDER +?BY +?%o:orderBy
         |?LIMIT +?%n:limit
         |""".stripMargin)

    // create the SELECT model
    var select: Queryable = Select(
      fields = params.expressions("fields"),
      from = params.sources.get("source") map (_.toQueryable),
      joins = params.joins.getOrElse("joins", Nil),
      where = params.conditions.get("condition"),
      groupBy = params.fields.getOrElse("groupBy", Nil),
      having = params.conditions.get("havingCondition"),
      orderBy = params.orderedFields.getOrElse("orderBy", Nil),
      limit = (params.numerics.get("limit") ?? params.numerics.get("top")).map(_.toInt))

    def getQuery(tsx: TokenStream): Queryable = SQLTemplateParams(tsx, "%Q:query").sources("query").toQueryable

    // is there an EXCEPT or UNION clause?
    val keywords = Seq("EXCEPT", "INTERSECT", "MINUS", "UNION")
    while (keywords.exists(stream is _)) {
      select = stream match {
        case ts if (ts nextIf "EXCEPT") | (ts nextIf "MINUS") => Except(query0 = select, query1 = getQuery(ts))
        case ts if ts nextIf "INTERSECT" => Intersect(query0 = select, query1 = getQuery(ts))
        case ts if ts nextIf "UNION" =>
          val params = SQLTemplateParams(ts, "?%C(mode|ALL|DISTINCT) %Q:query")
          val isDistinct = params.atoms.is("mode", _ equalsIgnoreCase "DISTINCT")
          Union(query0 = select, query1 = params.sources("query"), isDistinct)
        case ts => ts die "Expected EXCEPT, INTERSECT, MINUS or UNION"
      }
    }

    // is there an INTO or OVERWRITE clause?
    params.locations.get("target") map { target =>
      val isOverwrite = params.atoms.is("mode", _ equalsIgnoreCase "OVERWRITE")
      Insert(
        source = select,
        destination = if (isOverwrite) Insert.Overwrite(target) else Insert.Into(target),
        fields = params.expressions("fields") map {
          case field: Field => field
          case expr: NamedExpression => BasicField(expr.name)
          case expr => stream.die(s"Invalid field definition $expr")
        })
    } getOrElse select
  }

  /**
    * Parses a textual sequence
    * @param ts        the given [[TokenStream token stream]]
    * @param startElem the given starting element (e.g. "{")
    * @param endElem   the given ending element (e.g. "}")
    * @return an [[SQL code block]]
    */
  def parseSequence(ts: TokenStream, startElem: String, endElem: String): SQL = {
    SQL(ts.captureIf(startElem, endElem, delimiter = Some(";"))(parseNext))
  }

  /**
    * Parses a SET statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Invokable]]
    * @example {{{ SET @customers = ( SELECT * FROM Customers WHERE deptId = 31 ) }}}
    * @example {{{ SET $customers = $customers + 1 }}}
    */
  def parseSet(ts: TokenStream): Invokable = {
    val params = SQLTemplateParams(ts, "SET %v:variable =")
    params.variables("variable") match {
      case v: LocalVariableRef => SetLocalVariable(v.name, SQLTemplateParams(ts, "%e:expr").assignables("expr"))
      case v: RowSetVariableRef => SetRowVariable(v.name, SQLTemplateParams(ts, "%Q:expr").sources("expr"))
    }
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
  def parseShow(ts: TokenStream): Show = {
    val params = SQLTemplateParams(ts, "SHOW %V:rows ?LIMIT +?%n:limit")
    Show(rows = params.sources("rows"), limit = params.numerics.get("limit").map(_.toInt))
  }

  /**
   * Parses a TRUNCATE statement
   * @example
   * {{{
   *   TRUNCATE Companies
   * }}}
   * @param ts the given [[TokenStream token stream]]
   * @return a [[Truncate]]
   */
  def parseTruncate(ts: TokenStream): Truncate = {
    val params = SQLTemplateParams(ts, "TRUNCATE %L:target")
    Truncate(table = params.locations("target"))
  }

  /**
   * Parses an UPDATE statement
   * @example
   * {{{
   * UPDATE Companies
   * SET Symbol = 'AAPL', Name = 'Apple, Inc.', Sector = 'Technology', Industry = 'Computers', LastSale = 203.45
   * WHERE Symbol = 'AAPL'
   * LIMIT 20
   * }}}
   * @param ts the given [[TokenStream token stream]]
   * @return an [[Update]]
   */
  def parseUpdate(ts: TokenStream): Update = {
    val params = SQLTemplateParams(ts, "UPDATE %t:name SET %U:assignments ?WHERE +?%c:condition ?LIMIT +?%n:limit")
    Update(
      table = Table(params.atoms("name")),
      changes = params.keyValuePairs("assignments"),
      where = params.conditions.get("condition"),
      limit = params.numerics.get("limit").map(_.toInt))
  }

  /**
    * Parses a WHILE statement
    * @example
    * {{{
    * WHILE $cnt < 10
    * BEGIN
    *    PRINT 'Hello World';
    *    SET $cnt = $cnt + 1;
    * END;
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[While]]
    */
  def parseWhile(ts: TokenStream): While = {
    val params = SQLTemplateParams(ts, "WHILE %c:condition %N:command")
    While(condition = params.conditions("condition"), invokable = params.sources("command"))
  }

  /**
    * Determines the storage format
    * @param formatString the storage format class or string
    * @return the [[StorageFormat]]
    */
  private def determineStorageFormat(formatString: String): StorageFormat = formatString.toUpperCase() match {
    case s if s.contains("AVRO") => StorageFormats.AVRO
    case s if s.contains("CSV") => StorageFormats.CSV
    case s if s.contains("JDBC") => StorageFormats.JDBC
    case s if s.contains("JSON") => StorageFormats.JSON
    case s if s.contains("PARQUET") => StorageFormats.PARQUET
    case s if s.contains("ORC") => StorageFormats.ORC
    case s if s.contains("TEXT") => StorageFormats.CSV
    case _ => throw new IllegalArgumentException(s"Could not determine storage format from '$formatString'")
  }

}

/**
  * SQL Language Parser
  * @author lawrence.daniels@gmail.com
  */
object SQLLanguageParser {

  /**
    * Parses the contents of the given file into an [[Invokable invokable]]
    * @param file the given [[File file]]
    * @return the resultant [[Invokable]]
    */
  def parse(file: File): Invokable = parse(Source.fromFile(file).use(_.mkString))

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
  def parse(url: URL): Invokable = parse(Source.fromURL(url).use(_.mkString))

  /**
    * Parses the contents of the given string into an [[Invokable invokable]]
    * @param sourceCode the given SQL code string (e.g. "SELECT * FROM Customers")
    * @return the resultant [[Invokable]]
    */
  def parse(sourceCode: String): Invokable = {
    new SQLLanguageParser {} iterate TokenStream(sourceCode) toList match {
      case List(SQL(ops)) if ops.size == 1 => ops.head
      case op :: Nil => op
      case ops => SQL(ops)
    }
  }

}