package com.github.ldaniels528.qwery

import java.io.FileInputStream
import java.util.Properties

import com.github.ldaniels528.qwery.SQLLanguageParser.{SLPExtensions, die}
import com.github.ldaniels528.qwery.ops.NamedExpression._
import com.github.ldaniels528.qwery.ops.builtins.Return
import com.github.ldaniels528.qwery.ops.sql._
import com.github.ldaniels528.qwery.ops.{Expression, _}
import com.github.ldaniels528.qwery.sources.{DataResource, NamedResource}
import com.github.ldaniels528.qwery.util.OptionHelper._
import com.github.ldaniels528.qwery.util.PeekableIterator
import com.github.ldaniels528.qwery.util.ResourceHelper._

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * SQL Language Parser
  * @author lawrence.daniels@gmail.com
  */
class SQLLanguageParser(stream: TokenStream) extends ExpressionParser {

  /**
    * Indicates whether the given stream matches the given template
    * @param template the given template
    * @return true, if the stream matches the template from its current position
    */
  def matches(template: String): Boolean = {
    stream.mark()
    try new SQLLanguageParser(stream).process(template).nonEmpty catch {
      case _: Throwable => false
    } finally stream.reset()
  }

  /**
    * Extracts the tokens that correspond to the given template
    * @param template the given template (e.g. "INSERT INTO @table ( %F:fields ) VALUES ( %o:values )")
    * @return a mapping of the extracted values
    */
  def process(template: String): SQLTemplateParams = {
    var results = SQLTemplateParams()
    val tags = new PeekableIterator(template.split("[ ]").map(_.trim))
    while (tags.hasNext) {
      processNextTag(tags.next(), results, tags) match {
        case Success(params) => results = results + params
        case Failure(e) => throw SyntaxException(e.getMessage, stream)
      }
    }
    results
  }

  /**
    * Extracts and evaluates the next tag in the sequence
    * @param aTag the given tag (e.g. "@fields")
    * @param tags the [[PeekableIterator iteration]] of tags
    * @return the option of the resultant [[SQLTemplateParams template parameters]]
    */
  private def processNextTag(aTag: String, params: SQLTemplateParams, tags: PeekableIterator[String]): Try[SQLTemplateParams] = aTag match {
    // optionally required tag? (e.g. "?LIMIT +?%n:limit" => "LIMIT 100")
    case tag if tag.startsWith("?") => extractOptional(tag.drop(1), params, tags)

    // optionally required child-tag? (e.g. "?ORDER +?BY +?%o:sortFields" => "ORDER BY Symbol DESC")
    case tag if tag.startsWith("+?") => processNextTag(aTag = tag.drop(2), params, tags)

    // atom? (e.g. "%a:table" => "'./tickers.csv'")
    case tag if tag.startsWith("%a:") => extractIdentifier(tag.drop(3))

    // list of arguments? (e.g. "%A:args" => "(1,2,3)")
    case tag if tag.startsWith("%A:") => extractListOfArguments(tag.drop(3))

    // conditional expression? (e.g. "%c:condition" => "x = 1 and y = 2")
    case tag if tag.startsWith("%c:") => extractCondition(tag.drop(3))

    // chooser? (e.g. "%C(mode,INTO,OVERWRITE)" => "INSERT INTO ..." || "INSERT OVERWRITE ...")
    case tag if tag.startsWith("%C(") & tag.endsWith(")") => extractChosenItem(tag.chooserParams)

    // assignable expression? (e.g. "%e:expression" => "2 * (x + 1)")
    case tag if tag.startsWith("%e:") => extractAssignableExpression(tag.drop(3))

    // expressions? (e.g. "%E:fields" => "field1, 'hello', 5 + now(), ..., fieldN")
    case tag if tag.startsWith("%E:") => extractListOfExpressions(tag.drop(3))

    // field names? (e.g. "%F:fields" => "field1, field2, ..., fieldN")
    case tag if tag.startsWith("%F:") => extractListOfFields(tag.drop(3))

    // joins? (e.g. "%J:joins" => "INNER JOIN 'stocks.csv' ON A.symbol = B.ticker")
    case tag if tag.startsWith("%J:") => extractJoins(tag.drop(3), params)

    // numeric? (e.g. "%n:limit" => "100")
    case tag if tag.startsWith("%n:") => extractNumericValue(tag.drop(3))

    // ordered field list? (e.g. "%o:orderedFields" => "field1 DESC, field2 ASC")
    case tag if tag.startsWith("%o:") => extractOrderedColumns(tag.drop(3))

    // parameters? (e.g. "%P:params" => "name:String, date:Date")
    case tag if tag.startsWith("%P:") => extractListOfParameters(tag.drop(3))

    // query or expression? (e.g. "%q:queryExpr" => "x + 1" | "( SELECT firstName, lastName FROM AddressBook )")
    case tag if tag.startsWith("%q:") => extractQueryOrExpression(tag.drop(3))

    // query? (e.g. "%Q:query" => "SELECT firstName, lastName FROM AddressBook")
    case tag if tag.startsWith("%Q:") => extractQuery(tag.drop(3))

    // regular expression match? (e.g. "%r`\\d{3,4}S+`" => "123ABC")
    case tag if tag.startsWith("%r`") & tag.endsWith("`") => extractRegEx(pattern = tag.drop(3).dropRight(1))

    // repeat start/end tag? (e.g. "%R:valueSet {{ VALUES ( %E:values ) }}" => "VALUES (123, 456) VALUES (345, 678)")
    case tag if tag.startsWith("%R:") =>
      if (!tags.nextOption.contains("{{")) throw new IllegalArgumentException("Start of sequence '{{' expected")
      processRepeatedSequence(name = tag.drop(3), params, tags)

    // source or sub-query? (e.g. "%s:source" => "'AddressBook'" | "( SELECT firstName, lastName FROM AddressBook )")
    case tag if tag.startsWith("%s:") => extractSourceOrQuery(tag.drop(3))

    // update field assignments
    case tag if tag.startsWith("%U:") => extractFieldAssignmentExpressions(tag.drop(3))

    // variable reference? (e.g. "%v:variable" => "SET @variable = 5")
    case tag if tag.startsWith("%v:") => extractVariableReference(tag.drop(3))

    // insert values or selection? (e.g. "%V:insertSource" => "VALUES (1, 2, 3)" | "SELECT 1, 2, 3")
    case tag if tag.startsWith("%V:") => extractInsertSelection(tag.drop(3))

    // with hints? (e.g. "%w:hints" => "WITH JSON FORMAT")
    case tag if tag.startsWith("%w:") => extractWithClause(tag.drop(3))

    // executable? (e.g. "%X:code" => "CREATE PROCEDURE test AS SHOW FILES")
    case tag if tag.startsWith("%X:") => extractSQLCommand(tag.drop(3))

    // must be literal text (e.g. "FROM")
    case text => extractKeyWord(text)
  }

  /**
    * Extracts an assignable expression
    * @param name the given identifier name (e.g. "variable")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractAssignableExpression(name: String): Try[SQLTemplateParams] = Try {
    val expr = parseExpression(stream).getOrElse(die("Expression expected"))
    SQLTemplateParams(assignables = Map(name -> expr))
  }

  /**
    * Extracts a collection of field assignment expressions (e.g. "active = 1, ready = 0, total = subtotal + 1")
    * @param name the given identifier name (e.g. "fields")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractFieldAssignmentExpressions(name: String) = Try {
    var assignments: List[(String, Expression)] = Nil
    val template = "%a:name = %e:expression"
    do {
      val params = SQLTemplateParams(stream, template)
      assignments = params.atoms("name") -> params.assignables("expression") :: assignments
    } while (stream nextIf ",")
    SQLTemplateParams(keyValuePairs = Map(name -> assignments.reverse))
  }

  /**
    * Extracts an expression from the token stream
    * @param name the given identifier name (e.g. "condition")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractCondition(name: String): Try[SQLTemplateParams] = Try {
    val condition = parseCondition(stream)
    SQLTemplateParams(conditions = Map(name -> condition.getOrElse(die("Conditional expression expected"))))
  }

  /**
    * Extracts an enumerated item
    * @param values the values
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractChosenItem(values: Seq[String]) = Try {
    def error[A](items: List[String]) = die[A](s"One of the following '${items.mkString(", ")}' identifiers is expected")

    values.toList match {
      case name :: items =>
        val item = stream.peek.map(_.text).getOrElse(error(items))
        if (!items.exists(_.equalsIgnoreCase(item))) error(items)
        stream.next() // must skip the token
        SQLTemplateParams(atoms = Map(name -> item))
      case _ =>
        die(s"Unexpected template error: ${values.mkString(", ")}")
    }
  }

  /**
    * Extracts an identifier from the token stream
    * @param name the given identifier name (e.g. "source")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractIdentifier(name: String) = Try {
    val value = stream.peek.map(_.text).getOrElse(die(s"'$name' identifier expected"))
    stream.next()
    SQLTemplateParams(atoms = Map(name -> value))
  }

  private def extractInsertSelection(name: String) = Try {
    import SQLLanguageParser.{parseInsertValues, parseNext}
    val source = stream match {
      case ts if ts.is("VALUES") => parseInsertValues(ts)
      case ts => parseNext(ts)
    }
    SQLTemplateParams(sources = Map(name -> source))
  }

  /**
    * Extracts JOIN statements from the token stream
    * @example {{{ INNER JOIN './companylist.csv' AS A ON A.Symbol = B.Symbol }}}
    * @example {{{ JOIN './companylist.csv' AS A ON A.Symbol = B.Symbol }}}
    * @param name the given named identifier
    * @return an [[Describe executable]]
    */
  private def extractJoins(name: String, aggParams: SQLTemplateParams) = Try {
    var joins: List[Join] = Nil
    while (stream.is("JOIN") || stream.is("INNER") || stream.is("OUTER")) {
      stream match {
        case ts if ts.is("JOIN") | ts.nextIf("INNER") =>
          val params = SQLTemplateParams(ts, "JOIN %a:joinPath AS %a:joinAlias ON %c:joinCond %w:joinHints")
          joins = InnerJoin(
            leftAlias = aggParams.sources("source") match {
              case NamedResource(alias, _) => Some(alias)
              case _ => None
            },
            right = NamedResource(
              name = params.atoms("joinAlias"),
              resource = DataResource(params.atoms("joinPath"), hints = params.hints.get("joinHints"))
            ),
            condition = params.conditions("joinCond")) :: joins
        case _ => die("Syntax error: Invalid JOIN expression")
      }
    }
    SQLTemplateParams(joins = Map(name -> joins.reverse))
  }

  private def extractKeyWord(keyword: String) = Try {
    if (!stream.nextIf(keyword)) die(s"$keyword expected") else SQLTemplateParams()
  }

  /**
    * Extracts an arguments list from the token stream
    * @param name the given identifier name (e.g. "(customerId, 192, 'A')")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfArguments(name: String) = Try {
    var expressions: List[Expression] = Nil
    var done = false
    do {
      done = stream.is(")")
      if (!done) {
        expressions = parseExpression(stream).getOrElse(stream.die("Expression expected")) :: expressions
        if (!stream.is(")")) stream.expect(",")
      }
    } while (!done)
    SQLTemplateParams(expressions = Map(name -> expressions.reverse))
  }

  /**
    * Extracts a field list by name from the token stream
    * @param name the given identifier name (e.g. "fields")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfFields(name: String) = Try {
    var fields: List[Field] = Nil
    do fields = Field(stream) :: fields while (stream nextIf ",")
    SQLTemplateParams(fields = Map(name -> fields.reverse))
  }

  /**
    * Extracts an expression list from the token stream
    * @param name the given identifier name (e.g. "customerId, COUNT(*)")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfExpressions(name: String) = Try {

    def fetchNext(ts: TokenStream): Expression = {
      val expression = parseExpression(ts)
      val result = if (ts nextIf "AS") expression.map(parseNamedAlias(ts, _)) else expression
      result.getOrElse(die("Unexpected end of statement"))
    }

    var expressions: List[Expression] = Nil
    do expressions = expressions ::: fetchNext(stream) :: Nil while (stream nextIf ",")
    SQLTemplateParams(expressions = Map(name -> expressions))
  }

  /**
    * Extracts a parameter list from the token stream
    * @param name the given identifier name (e.g. "OUT customerId String, customerName String")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfParameters(name: String) = Try {
    val template = "?OUT %a:name %a:type"
    val parser = SQLLanguageParser(stream)
    var parameters: List[Field] = Nil
    var matched = false
    do {
      matched = !stream.is(")") && parser.matches(template)
      if (matched) {
        val params = parser.process(template)
        val name = params.atoms("name")
        val typeName = params.atoms("type")
        if (!Expression.isValidType(typeName)) die(s"Invalid data type '$typeName'")
        parameters = Field(name) :: parameters
      }
    } while (matched && stream.nextIf(","))

    SQLTemplateParams(fields = Map(name -> parameters.reverse))
  }

  /**
    * Extracts a numeric value from the token stream
    * @param name the given identifier name (e.g. "limit")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractNumericValue(name: String) = Try {
    val text = stream.peek.map(_.text).getOrElse(die(s"'$name' numeric expected"))
    Try(text.toDouble) match {
      case Success(value) =>
        stream.next()
        SQLTemplateParams(numerics = Map(name -> value))
      case Failure(_) => die(s"'$name' expected a numeric value")
    }
  }

  /**
    * Extracts an optional tag expression
    * @param tag  the tag to be executed (e.g. "%a:name")
    * @param tags the [[PeekableIterator iterator]]
    */
  private def extractOptional(tag: String, params: SQLTemplateParams, tags: PeekableIterator[String]): Try[SQLTemplateParams] = Try {
    processNextTag(aTag = tag, params, tags) match {
      case Success(result) => result
      case Failure(e) =>
        while (tags.peek.exists(_.startsWith("+?"))) {
          //logger.info(s"${e.getMessage} skipping: ${tags.peek}")
          tags.next()
        }
        SQLTemplateParams()
    }
  }

  /**
    * Extracts a list of sort columns from the token stream
    * @param name the given identifier name (e.g. "sortedColumns")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractOrderedColumns(name: String) = Try {
    var sortFields: List[OrderedColumn] = Nil
    do {
      val name = stream.nextOption.map(_.text).getOrElse(die("Unexpected end of statement"))
      val direction = stream match {
        case ts if ts nextIf "ASC" => true
        case ts if ts nextIf "DESC" => false
        case _ => true
      }
      sortFields = sortFields ::: OrderedColumn(name, direction) :: Nil
    } while (stream nextIf ",")
    SQLTemplateParams(orderedFields = Map(name -> sortFields))
  }

  /**
    * Parses a source expression; either a direct or via query
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractQuery(name: String) = Try {
    SQLTemplateParams(sources = Map(name -> extractQueryOption.getOrElse(die("Sub-query expected"))))
  }

  /**
    * Parses a source expression; either a direct or via query
    * @return the option of a [[Executable query resource]]
    */
  private def extractQueryOption: Option[Executable] = {
    import SQLLanguageParser._

    val outcome = stream match {
      case ts if ts is "SELECT" => Option(parseSelect(stream))
      case ts if ts nextIf "(" =>
        val result = Option(parseNext(stream))
        stream expect ")"
        result
      case _ => None
    }
    // is there as alias?
    outcome map { query =>
      if (stream nextIf "AS") NamedResource(name = stream.next().text, query) else query
    }
  }

  /**
    * Parses a source expression; either a direct or via query
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractQueryOrExpression(name: String) = Try {
    val expression = (extractQueryOption.map(_.toExpression) ?? parseExpression(stream))
      .getOrElse(die("Expression or sub-query was expected"))
    SQLTemplateParams(assignables = Map(name -> expression))
  }

  private def extractRegEx(pattern: String) = Try {
    if (stream.matches(pattern)) SQLTemplateParams() else die(s"Did not match the expected pattern '$pattern'")
  }

  /**
    * Parses a source expression; either direct (e.g. 'customers') or via query ("SELECT * FROM customers")
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractSourceOrQuery(name: String) = Try {
    val executable = stream match {
      case ts if ts nextIf "(" =>
        val result = SQLLanguageParser.parseNext(stream)
        stream expect ")"
        result
      case ts if ts.isQuoted => DataResource(ts.next().text)
      case _ => die("Source or sub-query expected")
    }
    // is there as alias?
    val resource = if (stream nextIf "AS") NamedResource(name = stream.next().text, executable) else executable
    SQLTemplateParams(sources = Map(name -> resource))
  }

  private def extractSQLCommand(name: String) = Try {
    SQLTemplateParams(sources = Map(name -> SQLLanguageParser.parseNext(stream)))
  }

  private def extractRepeatedSequence(tags: Iterator[String]) = {
    tags.takeWhile(_ != "}}").toSeq
  }

  private def extractVariableReference(name: String) = Try {
    val reference = stream match {
      case ts if ts nextIf "@" => VariableRef(ts.next().text)
      case ts => die("Variable expected")
    }
    SQLTemplateParams(variables = Map(name -> reference))
  }

  /**
    * Extracts WITH clauses
    * @example WITH CSV|PSV|TSV|JSON FORMAT
    * @example WITH DELIMITER ','
    * @example WITH QUOTED NUMBERS
    * @example WITH GZIP COMPRESSION
    * @param name the name of the collection
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractWithClause(name: String) = Try {
    val withAvro = "WITH AVRO %a:avro"
    val withCompression = "WITH %C(compression,GZIP) COMPRESSION"
    val withDelimiter = "WITH DELIMITER %a:delimiter"
    val withFixed = "WITH FIXED %a:fixedType"
    val withFormat = "WITH %C(format,CSV,JSON,PSV,TSV) FORMAT"
    val withJdbcDriver = "WITH JDBC DRIVER %a:jdbcDriver"
    val withJsonPath = "WITH JSON PATH ( %E:jsonPath )"
    val withHeader = "WITH COLUMN %C(column,HEADERS)"
    val withProps = "WITH PROPERTIES %a:props"
    val withQuoted = "WITH QUOTED %C(quoted,NUMBERS,TEXT)"
    val withSparkMaster = "WITH SPARK MASTER %a:master"
    val parser = SQLLanguageParser(stream)
    var hints = Hints()

    def toOption(on: Boolean) = if (on) Some(true) else None

    while (stream.is("WITH")) {
      parser match {
        // WITH AVRO ['./twitter.avsc']
        case p if p.matches(withAvro) =>
          val params = p.process(withAvro)
          hints = hints.copy(avro = params.atoms.get("avro").map(getContents))
        // WITH COLUMN [HEADERS]
        case p if p.matches(withHeader) =>
          val params = p.process(withHeader)
          hints = hints.copy(headers = params.atoms.get("column").map(_.equalsIgnoreCase("HEADERS")))
        // WITH FIXED WIDTHS
        case p if p.matches(withFixed) =>
          val params = p.process(withFixed)
          if (!params.atoms.get("fixedType").exists(_.equalsIgnoreCase("WIDTH"))) stream.die("Keyword WIDTH expected")
          hints = hints.copy(fixed = Some(true))
        // WITH [GZIP] COMPRESSION
        case p if p.matches(withCompression) =>
          val params = p.process(withCompression)
          hints = hints.copy(gzip = params.atoms.get("compression").map(_.equalsIgnoreCase("GZIP")))
        // WITH DELIMITER [,]
        case p if p.matches(withDelimiter) =>
          val params = p.process(withDelimiter)
          hints = hints.copy(delimiter = params.atoms.get("delimiter"))
        // WITH JDBC 'com.mysql.jdbc.Driver'
        case p if p.matches(withJdbcDriver) =>
          val params = p.process(withJdbcDriver)
          hints = hints.copy(jdbcDriver = params.atoms.get("jdbcDriver"))
        // WITH JSON PATH ( elem1, elem2, .., elemN )
        case p if p.matches(withJsonPath) =>
          val params = p.process(withJsonPath)
          hints = hints.copy(jsonPath = params.expressions("jsonPath"))
        // WITH PROPERTIES ['./settings.properties']
        case p if p.matches(withProps) =>
          val params = p.process(withProps)
          hints = hints.copy(properties = params.atoms.get("props").map(getProperties))
        // WITH QUOTED [NUMBERS|TEXT]
        case p if p.matches(withQuoted) =>
          val params = p.process(withQuoted)
          hints = hints.copy(
            quotedNumbers = toOption(params.atoms.get("quoted").exists(_.equalsIgnoreCase("NUMBERS"))) ?? hints.quotedNumbers,
            quotedText = toOption(params.atoms.get("quoted").exists(_.equalsIgnoreCase("TEXT"))) ?? hints.quotedText)
        // WITH [CSV|PSV|TSV|JSON] FORMAT
        case p if p.matches(withFormat) =>
          val params = p.process(withFormat)
          params.atoms.get("format").foreach(format => hints = hints.usingFormat(format = format))
        // WITH SPARK MASTER ['local']
        case p if p.matches(withSparkMaster) =>
          val params = p.process(withSparkMaster)
          params.atoms.get("master").foreach(master => hints = hints.copy(sparkMaster = Some(master)))
        case _ =>
          die("Syntax error")
      }
    }
    SQLTemplateParams(hints = if (hints.nonEmpty) Map(name -> hints) else Map.empty)
  }

  private def getContents(path: String): String = Source.fromFile(path).mkString

  private def getProperties(path: String): Properties = {
    val props = new Properties()
    new FileInputStream(path) use props.load
    props
  }

  /**
    * Extracts a repeated sequence from the stream
    * @param name the named identifier
    * @param tags the [[PeekableIterator iterator]]
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def processRepeatedSequence(name: String, params: SQLTemplateParams, tags: PeekableIterator[String]) = Try {
    // extract the repeated sequence
    val repeatedTagsSeq = extractRepeatedSequence(tags)
    var paramSet: List[SQLTemplateParams] = Nil
    var done = false
    while (!done && stream.hasNext) {
      var result: Try[SQLTemplateParams] = Success(SQLTemplateParams())
      val count = paramSet.size
      val repeatedTags = new PeekableIterator(repeatedTagsSeq)
      while (repeatedTags.hasNext) {
        result = processNextTag(repeatedTags.next(), params, repeatedTags)
        result.foreach(params => paramSet = paramSet ::: params :: Nil)
      }

      // if we didn't add anything, stop.
      done = paramSet.size == count
    }
    SQLTemplateParams(repeatedSets = Map(name -> paramSet))
  }

}

/**
  * SQL Language Parser Singleton
  * @author lawrence.daniels@gmail.com
  */
object SQLLanguageParser {

  /**
    * Creates a new SQL Language Parser instance
    * @param ts the given [[TokenStream token stream]]
    * @return the [[SQLLanguageParser language parser]]
    */
  def apply(ts: TokenStream): SQLLanguageParser = new SQLLanguageParser(ts)

  /**
    * Returns an iterator of executables
    * @param stream the given [[TokenStream token stream]]
    * @return an iterator of [[Executable executables]]
    */
  def parseFully(stream: TokenStream) = new Iterator[Executable] {

    override def hasNext: Boolean = {
      while (stream.hasNext && stream.is(";")) stream.next()
      stream.hasNext
    }

    override def next(): Executable = parseNext(stream)
  }

  /**
    * Parses the next query or statement from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Executable]]
    */
  def parseNext(stream: TokenStream): Executable = stream match {
    case ts if ts is "BEGIN" => parseBegin(ts)
    case ts if ts is "CALL" => parseCall(ts)
    case ts if ts is "CREATE" => parseCreate(ts)
    case ts if ts is "DECLARE" => parseDeclare(ts)
    case ts if ts is "DESCRIBE" => parseDescribe(ts)
    case ts if ts is "INSERT" => parseInsert(ts)
    case ts if ts is "UPDATE" => parseUpdate(ts)
    case ts if ts is "UPSERT" => parseUpsert(ts)
    case ts if ts is "NATIVE SQL" => parseNativeSQL(ts)
    case ts if ts is "RETURN" => parseReturn(ts)
    case ts if ts is "SELECT" => parseSelect(ts)
    case ts if ts is "SET" => parseSet(ts)
    case ts if ts is "SHOW" => parseShow(ts)
    case ts if ts nextIf "SPARK" => parseSpark(ts)
    case ts => die(s"Unrecognized command near '${ts.peek.map(_.text).orNull}'")
  }

  /**
    * Parses a BEGIN .. END statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[CodeBlock executable]]
    */
  private def parseBegin(ts: TokenStream): CodeBlock = {
    var operations: List[Executable] = Nil
    if (ts.nextIf("BEGIN")) {
      while (ts.hasNext && !ts.nextIf("END")) {
        operations = operations ::: parseNext(ts) :: Nil
        if (!ts.is("END")) ts.expect(";")
      }
    }
    CodeBlock(operations)
  }

  /**
    * Parses a CALL statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Call executable]]
    */
  private def parseCall(ts: TokenStream): Call = {
    val params = SQLTemplateParams(ts, "CALL %a:name ?( +?%A:args +?)")
    Call(name = params.atoms("name"), args = params.expressions.getOrElse("args", Nil))
  }

  /**
    * Parses a CREATE statement
    * @param stream the given [[TokenStream token stream]]
    * @return an [[View executable]]
    */
  private def parseCreate(stream: TokenStream): Executable = {
    stream match {
      case ts if ts is "CREATE FUNCTION" => parseCreateFunction(ts)
      case ts if ts is "CREATE PROCEDURE" => parseCreateProcedure(ts)
      case ts if ts is "CREATE VIEW" => parseCreateView(ts)
      case _ => throw new IllegalArgumentException("Expected keyword FUNCTION, PROCEDURE or VIEW")
    }
  }

  /**
    * Parses a CREATE FUNCTION statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Procedure executable]]
    */
  private def parseCreateFunction(ts: TokenStream): Function = {
    val params = SQLTemplateParams(ts, "CREATE FUNCTION %a:name ?( +?%P:params +?) AS %X:code")
    Function(
      name = params.atoms("name"),
      parameters = params.fields.getOrElse("params", Nil),
      executable = params.sources("code"))
  }

  /**
    * Parses a CREATE PROCEDURE statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Procedure executable]]
    */
  private def parseCreateProcedure(ts: TokenStream): Procedure = {
    val params = SQLTemplateParams(ts, "CREATE PROCEDURE %a:name ?( +?%P:params +?) AS %X:code")
    Procedure(
      name = params.atoms("name"),
      parameters = params.fields.getOrElse("params", Nil),
      executable = params.sources("code"))
  }

  /**
    * Parses a CREATE VIEW statement
    * @param ts the given [[TokenStream token stream]]
    * @return an [[View executable]]
    */
  private def parseCreateView(ts: TokenStream): View = {
    val params = SQLTemplateParams(ts, "CREATE VIEW %a:name AS %Q:query")
    View(name = params.atoms("name"), query = params.sources("query"))
  }

  /**
    * Parses a DECLARE statement
    * @example {{{ DECLARE @counter DOUBLE }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Declare executable]]
    */
  private def parseDeclare(ts: TokenStream): Declare = {
    val params = SQLTemplateParams(ts, "DECLARE %v:name %a:type")
    val typeName = params.atoms("type")
    if (!Expression.isValidType(typeName)) die(s"Invalid type '$typeName'")
    Declare(variableRef = params.variables("name"), typeName = typeName)
  }

  /**
    * Parses a DESCRIBE statement
    * @example {{{ DESCRIBE './companylist.csv' }}}
    * @example {{{ DESCRIBE './companylist.csv' LIMIT 5 }}}
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Describe executable]]
    */
  private def parseDescribe(ts: TokenStream): Describe = {
    val params = SQLTemplateParams(ts, "DESCRIBE %s:source ?LIMIT +?%n:limit")
    Describe(
      source = params.sources.getOrElse("source", die("No source provided")),
      limit = params.numerics.get("limit").map(_.toInt))
  }

  /**
    * Parses an INSERT statement
    * @example
    * {{{
    * INSERT INTO './tickers.csv' (symbol, exchange, lastSale)
    * VALUES ('AAPL', 'NASDAQ', 145.67)
    * VALUES ('AMD', 'NYSE', 5.66)
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
  private def parseInsert(stream: TokenStream, spark: Boolean = false): Executable = {
    val params = SQLTemplateParams(stream, "INSERT %C(mode,INTO,OVERWRITE) %a:target ( %F:fields ) %w:hints %V:source")
    val append = params.atoms("mode").equalsIgnoreCase("INTO")
    val fields = params.fields("fields")
    val hints = (params.hints.get("hints") ?? Option(Hints())).map(_.copy(append = Some(append), fields = fields))
    if (spark)
      InsertSpark(
        target = DataResource(params.atoms("target"), hints),
        source = params.sources("source"),
        fields = fields)
    else
      Insert(
        target = DataResource(params.atoms("target"), hints),
        source = params.sources("source"),
        fields = fields)
  }

  /**
    * Parses an INSERT VALUES clause
    * @param ts the [[TokenStream token stream]]
    * @return the resulting [[InsertValues modifications]]
    */
  private def parseInsertValues(ts: TokenStream): InsertValues = {
    val valueSets = SQLTemplateParams(ts, "%R:valueSet {{ VALUES ( %E:values ) }}").repeatedSets.get("valueSet") match {
      case Some(sets) => sets.flatMap(_.expressions.get("values"))
      case None => ts.die("VALUES clause could not be parsed")
    }
    InsertValues(valueSets)
  }

  /**
    * Parses a NATIVE SQL statement
    * @param ts the [[TokenStream token stream]]
    * @return an [[NativeSQL executable]]
    */
  private def parseNativeSQL(ts: TokenStream): Executable = {
    val params = SQLTemplateParams(ts, "NATIVE SQL %e:sql FROM %a:jdbcUrl %w:hints")
    NativeSQL(query = params.assignables("sql"), jdbcUrl = params.atoms("jdbcUrl"), hints = params.hints.get("hints"))
  }

  /**
    * Parses a RETURN statement
    * @example {{{ RETURN }}}
    * @example {{{ RETURN "Hello World" }}}
    * @param ts the [[TokenStream token stream]]
    * @return an [[Return executable]]
    */
  private def parseReturn(ts: TokenStream): Return = {
    val params = SQLTemplateParams(ts, "RETURN ?%e:expression")
    Return(params.assignables.get("expression"))
  }

  /**
    * Parses a SELECT query
    * @example
    * {{{
    * SELECT symbol, exchange, lastSale FROM './EOD-20170505.txt'
    * WHERE exchange = 'NASDAQ'
    * LIMIT 5
    * }}}
    * @example
    * {{{
    * SELECT A.symbol, A.exchange, A.lastSale AS lastSaleA, B.lastSale AS lastSaleB
    * FROM 'companlist.csv' AS A
    * INNER JOIN 'companlist2.csv' AS B ON B.Symbol = A.Symbol
    * WHERE A.exchange = 'NASDAQ'
    * LIMIT 5
    * }}}
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Select executable]]
    */
  private def parseSelect(stream: TokenStream): Executable = {
    val params = SQLTemplateParams(stream,
      """
        |SELECT ?TOP +?%n:top %E:fields
        |?%C(mode,INTO,OVERWRITE) +?%a:target +?%w:targetHints
        |?FROM +?%s:source +?%w:sourceHints %J:joins
        |?WHERE +?%c:condition
        |?GROUP +?BY +?%F:groupBy
        |?ORDER +?BY +?%o:orderBy
        |?LIMIT +?%n:limit""".stripMargin)

    // create the SELECT statement
    var select: Executable = Select(
      fields = params.expressions("fields"),
      source = params.sources.get("source").map(_.withHints(params.hints.get("sourceHints"))),
      condition = params.conditions.get("condition"),
      groupFields = params.fields.getOrElse("groupBy", Nil),
      joins = params.joins.getOrElse("joins", Nil),
      orderedColumns = params.orderedFields.getOrElse("orderBy", Nil),
      limit = (params.numerics.get("limit") ?? params.numerics.get("top")).map(_.toInt))

    // is it a union?
    while (stream is "UNION") {
      val params = SQLTemplateParams(stream, "UNION %Q:union")
      select = Union(select, params.sources("union"))
    }

    // determine whether 'SELECT INTO' or 'SELECT OVERWRITE' was requested
    params.atoms.get("mode") match {
      case Some(mode) =>
        val append = mode.equalsIgnoreCase("INTO")
        val hints = params.hints.get("targetHints") ?? Option(Hints()) map (_.copy(append = Some(append)))
        Insert(
          target = DataResource(params.atoms("target"), hints),
          source = select,
          fields = params.expressions("fields") map {
            case field: Field => field
            case named: NamedExpression => Field(named.name)
            case expr => Field(expr.getName)
          })
      case None => select
    }
  }

  /**
    * Parses a variable assignment
    * @example {{{ SET @counter = 2 * x + 5 }}}
    * @example {{{ SET @counter = SELECT 1 }}}
    * @param ts the given [[TokenStream token stream]]
    * @return a [[Assignment variable assignment]]
    */
  private def parseSet(ts: TokenStream): Assignment = {
    val params = SQLTemplateParams(ts, "SET %v:name = %q:expression")
    Assignment(reference = params.variables("name"), value = params.assignables("expression"))
  }

  /**
    * Parses a SHOW command
    * @example {{{ SHOW VIEWS }}}
    * @param ts the given [[TokenStream token stream]]
    * @return a [[Show executable]]
    */
  private def parseShow(ts: TokenStream): Show = {
    val params = SQLTemplateParams(ts, "SHOW %a:entityType")
    val entityType = params.atoms("entityType")
    if (!Show.isValidEntityType(entityType)) die(s"Invalid entity type '$entityType'")
    Show(entityType)
  }

  /**
    * Parses the next query or statement from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Executable]]
    */
  def parseSpark(stream: TokenStream): Executable = stream match {
    case ts if ts is "INSERT" => parseInsert(ts, spark = true)
    case ts => die(s"Unrecognized Spark command near '${ts.peek.map(_.text).orNull}'")
  }

  /**
    * Parses an UPDATE statement
    * @example
    * {{{
    * UPDATE 'jdbc:mysql://localhost:3306/test?table=company'
    * SET Active = 1, Processed = 0
    * KEYED ON Symbol
    * WITH JDBC DRIVER 'com.mysql.jdbc.Driver'
    * SELECT Symbol, Name, Sector, Industry, `Summary Quote`
    * FROM 'companylist.csv' WITH FORMAT CSV
    * WHERE Industry = 'Oil/Gas Transmission'
    * }}}
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Update executable]]
    */
  private def parseUpdate(stream: TokenStream): Update = {
    val params = SQLTemplateParams(stream, "UPDATE %a:target SET %U:assignments KEYED ON %F:keyFields %w:hints %V:source")
    Update(
      target = DataResource(params.atoms("target"), params.hints.get("hints")),
      source = params.sources("source"),
      assignments = params.keyValuePairs("assignments"),
      keyedOn = params.fields("keyFields"))
  }

  /**
    * Parses an UPSERT statement
    * @example
    * {{{
    * UPSERT INTO 'jdbc:mysql://localhost:3306/test?table=company' (Symbol, Name, Sector, Industry, LastTrade)
    * KEYED ON Symbol
    * WITH JDBC DRIVER 'com.mysql.jdbc.Driver'
    * SELECT Symbol, Name, Sector, Industry, LastSale
    * FROM 'companylist.csv'
    * WHERE Symbol = #Symbol
    * }}}
    * @example
    * {{{
    * UPSERT INTO 'jdbc:mysql://localhost:3306/test?table=company' (Symbol, Name, Sector, Industry, LastTrade)
    * KEYED ON Symbol
    * WITH JDBC DRIVER 'com.mysql.jdbc.Driver'
    * VALUES ('CQH','Cheniere Energy Partners LP Holdings, LLC','Public Utilities','Oil/Gas Transmission', 25.68)
    * }}}
    */
  private def parseUpsert(stream: TokenStream): Upsert = {
    val params = SQLTemplateParams(stream, "UPSERT INTO %a:target ( %F:fields ) KEYED ON %F:keyFields %w:hints %V:source")
    Upsert(
      target = DataResource(params.atoms("target"), params.hints.get("hints")),
      source = params.sources("source"),
      fields = params.fields("fields"),
      keyedOn = params.fields("keyFields"))
  }

  private def die[A](message: String): A = throw new IllegalStateException(message)

  /**
    * SQL Language Parser Extensions
    * @param tag the given tag
    */
  final implicit class SLPExtensions(val tag: String) extends AnyVal {

    /**
      * Extracts the chooser parameters (e.g. "%C(mode,INTO,OVERWRITE)" => ["mode", "INTO", "OVERWRITE"])
      */
    @inline
    def chooserParams: Array[String] = tag.drop(3).dropRight(1).filterNot(_ == ' ').split(',').map(_.trim)
  }

}
