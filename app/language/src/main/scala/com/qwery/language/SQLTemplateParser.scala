package com.qwery.language

import com.qwery.language.SQLTemplateParser._
import com.qwery.models.Insert.DataRow
import com.qwery.models.JoinTypes.JoinType
import com.qwery.models._
import com.qwery.models.expressions._
import com.qwery.util.StringHelper._

import scala.util.{Failure, Success, Try}

/**
  * SQL Template Parser
  * @author lawrence.daniels@gmail.com
  */
class SQLTemplateParser(stream: TokenStream) extends ExpressionParser with SQLLanguageParser {

  /**
    * Indicates whether the given stream matches the given template
    * @param template the given template
    * @return true, if the stream matches the template from its current position
    */
  def matches(template: String): Boolean = {
    stream.mark()
    try new SQLTemplateParser(stream).process(template).nonEmpty catch {
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
        case Failure(e) => stream.die(e.getMessage, e)
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
    case tag if tag.startsWith("?") => extractOptional(tag drop 1, params, tags)

    // optionally required child-tag? (e.g. "?ORDER +?BY +?%o:sortFields" => "ORDER BY Symbol DESC")
    case tag if tag.startsWith("+?") => processNextTag(tag drop 2, params, tags)

    // atom? (e.g. "%a:name" => "Tickers" | "'Tickers'")
    case tag if tag.startsWith("%a:") => extractIdentifier(tag drop 3)

    // list of arguments? (e.g. "%A:args" => "(1,2,3)")
    case tag if tag.startsWith("%A:") => extractListOfArguments(tag drop 3)

    // conditional expression? (e.g. "%c:condition" => "x = 1 and y = 2")
    case tag if tag.startsWith("%c:") => extractCondition(tag drop 3)

    // chooser? (e.g. "%C(mode,INTO,OVERWRITE)" => "INSERT INTO ..." | "INSERT OVERWRITE ...")
    case tag if tag.startsWith("%C(") & tag.endsWith(")") => extractChosenItem(tag.chooserParams)

    // assignable expression? (e.g. "%e:expression" => "2 * (x + 1)")
    case tag if tag.startsWith("%e:") => extractAssignableExpression(tag drop 3)

    // expressions? (e.g. "%E:fields" => "field1, 'hello', 5 + now(), ..., fieldN")
    case tag if tag.startsWith("%E:") => extractListOfExpressions(tag drop 3)

    // field names? (e.g. "%F:fields" => "field1, field2, ..., fieldN")
    case tag if tag.startsWith("%F:") => extractListOfFields(tag drop 3)

    // I/O format? (e.g. "%f:format" => "INPUTFORMAT 'CSV'" | "OUTPUTFORMAT 'JSON'")
    case tag if tag.startsWith("%f:") => extractStorageFormat(tag drop 3)

    // joins? (e.g. "%J:joins" => "INNER JOIN 'stocks.csv' ON A.symbol = B.ticker")
    case tag if tag.startsWith("%J:") => extractJoins(tag drop 3, params)

    // keyword? (e.g. "%k:LOCATION" => Set("LOCATION"))
    case tag if tag.startsWith("%k:") => extractKeyword(tag drop 3)

    // location or table? (e.g. "TABLE accounts" | "LOCATION './temp/customers/csv')
    case tag if tag.startsWith("%L:") => extractLocationOrTable(tag drop 3)

    // location or table or sub-query? (e.g. "TABLE accounts" | "LOCATION './temp/customers/csv' | (SELECT ...))
    case tag if tag.startsWith("%LQ:") => extractLocationOrTableOrSubQuery(tag drop 4)

    // numeric? (e.g. "%n:limit" => "100")
    case tag if tag.startsWith("%n:") => extractNumericValue(tag drop 3)

    // next statement?
    case tag if tag.startsWith("%N:") => extractNextStatement(tag drop 3)

    // ordered field list? (e.g. "%o:orderedFields" => "field1 DESC, field2 ASC")
    case tag if tag.startsWith("%o:") => extractOrderedColumns(tag drop 3)

    // parameters? (e.g. "%P:params" => "name STRING, age INTEGER, dob DATE")
    case tag if tag.startsWith("%P:") => extractListOfParameters(tag drop 3)

    // properties? (e.g. "('quoteChar'='~', 'separatorChar'=',')")
    case tag if tag.startsWith("%p:") => extractProperties(tag drop 3)

    // indirect query source (queries, tables and variables)? (e.g. "%q:source" => "AddressBook" | "( SELECT * FROM AddressBook )" | "@addressBook")
    case tag if tag.startsWith("%q:") => extractQueryTableOrVariable(tag drop 3)

    // direct query source (queries and variables)? (e.g. "%Q:query" => "SELECT * FROM AddressBook" | "@addressBook")
    case tag if tag.startsWith("%Q:") => extractQueryOrVariable(tag drop 3)

    // regular expression match? (e.g. "%r`\\d{3,4}\\S+`" => "123ABC")
    case tag if tag.startsWith("%r`") & tag.endsWith("`") => extractRegEx(pattern = tag.drop(3).dropRight(1))

    // repeated sequence tag? (e.g. "%R:valueSet {{ VALUES ( %E:values ) }}" => "VALUES (123, 456) VALUES (345, 678)")
    case tag if tag.startsWith("%R:") => extractRepeatedSequence(name = tag drop 3, params, tags)

    // table tag? (e.g. "Customers")
    case tag if tag.startsWith("%t:") => extractTable(tag drop 3)

    // update field assignments
    case tag if tag.startsWith("%U:") => extractFieldAssignmentExpressions(tag drop 3)

    // variable reference? (e.g. "%v:variable" => "SET @variable = 5")
    case tag if tag.startsWith("%v:") => extractVariableReference(tag drop 3)

    // insert values (queries, VALUES and variables)? (e.g. "%V:data" => "(SELECT ...)" | "VALUES (...)" | "@numbers")
    case tag if tag.startsWith("%V:") => extractInsertSource(tag drop 3)

    // TABLE ... WITH clause
    case tag if tag.startsWith("%w:") => extractWithForTableClause(tag drop 3)

    // quoted text values (e.g. "%z:comment" => "'This is a comment'"
    case tag if tag.startsWith("%z:") => extractQuotedText(tag drop 3)

    // must be literal text (e.g. "FROM")
    case text => expectKeyword(text)
  }

  /**
    * Extracts an assignable expression
    * @param name the given identifier name (e.g. "variable")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractAssignableExpression(name: String): Try[SQLTemplateParams] = Try {
    val expr = parseExpression(stream).getOrElse(stream.die("Expression expected"))
    SQLTemplateParams(assignables = Map(name -> expr))
  }

  /**
    * Extracts a collection of field assignment expressions (e.g. "active = 1, ready = 0, total = subtotal + 1")
    * @param name the given identifier name (e.g. "fields")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractFieldAssignmentExpressions(name: String): Try[SQLTemplateParams] = Try {
    var assignments: List[(String, Expression)] = Nil
    do {
      val params = SQLTemplateParams(stream, template = "%a:name = %e:expression")
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
    SQLTemplateParams(conditions = Map(name -> (parseCondition(stream) getOrElse stream.die("Conditional expression expected"))))
  }

  /**
    * Extracts an enumerated item
    * @param values the values
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractChosenItem(values: Seq[String]): Try[SQLTemplateParams] = Try {
    @inline def error[A](items: List[String]): A =
      stream.die[A](s"One of the following '${items.map(s => s"'$s'").mkString(", ")}' identifiers is expected")

    values.toList match {
      case name :: items =>
        val item = stream.peek.map(_.text).getOrElse(error(items))
        if (!items.exists(_ equalsIgnoreCase item)) error(items)
        stream.next() // must skip the token
        SQLTemplateParams(atoms = Map(name -> item))
      case _ =>
        stream.die(s"Unexpected template error: ${values.mkString(", ")}")
    }
  }

  /**
    * Extracts an identifier from the token stream
    * @param name the given identifier name (e.g. "source")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractIdentifier(name: String) = Try {
    val value = stream.peek.map(_.text).getOrElse(stream.die(s"'$name' identifier expected"))
    stream.next()
    SQLTemplateParams(atoms = Map(name -> value))
  }

  /**
    * Extracts a VALUES clause from the token stream
    * @example {{{ VALUES @data }}}
    * @example {{{ VALUES ('Helen Choi', 41), ('Roger Choi', 13) }}}
    * @param name the given named identifier
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractInsertSource(name: String) = Try {
    val source = stream match {
      // VALUES clause?
      case ts if ts nextIf "VALUES" =>
        ts match {
          case ts1 if ts1 nextIf "@" => @@(ts1.next().text)
          case ts1 if ts1 nextIf "$" => ts1.die("Local variable references are not compatible with row sets")
          case ts1 =>
            var values: List[DataRow] = Nil
            do values = SQLTemplateParams(ts1, "( %E:values )").expressions("values") :: values while (ts1 nextIf ",")
            Insert.Values(values.reverse)
        }
      // variable?
      case ts if ts nextIf "@" => @@(ts.next().text)
      case ts if ts nextIf "$" => ts.die("Local variable references are not compatible with row sets")
      // any supported query ...
      case ts => parseNextQueryOrVariable(ts)
    }
    SQLTemplateParams(sources = Map(name -> source))
  }

  /**
    * Extracts JOIN statements from the token stream
    * @example {{{ INNER JOIN TABLE Customers AS A ON A.Symbol = B.Symbol }}}
    * @example {{{ LEFT JOIN LOCATION './companylist.csv' AS A ON A.Symbol = B.Symbol }}}
    * @param name the given named identifier
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractJoins(name: String, aggParams: SQLTemplateParams) = Try {
    val predicates = Seq("CROSS", "FULL", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER")
    var joins: List[Join] = Nil

    def join(ts: TokenStream, `type`: JoinType): Join = {
      val params = SQLTemplateParams(ts, "%LQ:source ON %c:condition")
      Join(
        source = if (params.locations.contains("source")) params.locations("source") else params.sources("source"),
        condition = params.conditions("condition"),
        `type` = `type`)
    }

    while (predicates.exists(stream is _)) {
      stream match {
        case ts if ts nextIf "CROSS JOIN" =>
          joins = join(ts, `type` = JoinTypes.CROSS) :: joins
        case ts if (ts nextIf "FULL JOIN") | (ts nextIf "FULL OUTER JOIN") =>
          joins = join(ts, `type` = JoinTypes.FULL_OUTER) :: joins
        case ts if (ts nextIf "LEFT JOIN") | (ts nextIf "LEFT OUTER JOIN") =>
          joins = join(ts, `type` = JoinTypes.LEFT_OUTER) :: joins
        case ts if (ts nextIf "RIGHT JOIN") | (ts nextIf "RIGHT OUTER JOIN") =>
          joins = join(ts, `type` = JoinTypes.RIGHT_OUTER) :: joins
        case ts if (ts nextIf "JOIN") | (ts nextIf "INNER JOIN") =>
          joins = join(ts, `type` = JoinTypes.INNER) :: joins
        case ts => ts.die("Syntax error: Invalid JOIN expression")
      }
    }
    SQLTemplateParams(joins = Map(name -> joins.reverse))
  }

  private def expectKeyword(keyword: String) = Try {
    if (!stream.nextIf(keyword)) stream.die(s"$keyword expected") else SQLTemplateParams()
  }

  /**
    * Extracts a keyword to indicate that the keyword was specified
    * @param keyword the given keyword (e.g. "OVERWRITE", "LOCATION")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractKeyword(keyword: String) = Try {
    if (stream nextIf keyword) SQLTemplateParams(keywords = Set(keyword.toUpperCase)) else SQLTemplateParams()
  }

  /**
    * Extracts an arguments list from the token stream
    * @param name the given identifier name (e.g. "(customerId, 192, 'A')")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfArguments(name: String) = Try {
    var expressions: List[Expression] = Nil
    var done = false

    stream.expect("(")
    do {
      done = stream is ")"
      if (!done) {
        expressions = parseExpression(stream).getOrElse(stream.die("Expression expected")) :: expressions
        if (stream isnt ")") stream.expect(",")
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
    do fields = parseField(stream) :: fields while (stream nextIf ",")
    SQLTemplateParams(fields = Map(name -> fields.reverse))
  }

  /**
    * Extracts an expression list from the token stream
    * @param name the given identifier name (e.g. "customerId, COUNT(*)")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfExpressions(name: String) = Try {

    def fetchNext(ts: TokenStream): Expression = {
      var expression: Option[Expression] = parseExpression(ts)
      if (ts nextIf "OVER") expression = expression.map(parseOver(_, ts))
      if (ts nextIf "AS") expression = expression.map(_.as(ts.next().text))
      expression.getOrElse(ts die "Unexpected end of statement")
    }

    var expressions: List[Expression] = Nil
    do expressions = fetchNext(stream) :: expressions while (stream nextIf ",")
    SQLTemplateParams(expressions = Map(name -> expressions.reverse))
  }

  /**
    * Parses a Window function
    * @param expression the given expression; usually a function to window over
    * @param stream     the given [[TokenStream token stream]]
    * @return the option of a [[Over]]
    * @example
    * {{{
    *   OVER (
    *     [ <PARTITION BY clause> ]
    *     [ <ORDER BY clause> ]
    *     [ <ROW or RANGE clause> ]
    *   )
    * }}}
    */
  private def parseOver(expression: Expression, stream: TokenStream): Expression = {
    // create an empty OVER clause
    var clause = Over(expression = expression)

    // process the inside of the parenthesis block
    stream expect "("
    while (stream isnt ")") {
      stream match {
        case ts if ts nextIf "ORDER BY" =>
          clause = clause.copy(orderBy = SQLTemplateParams(ts, "%o:orderBy").orderedFields("orderBy"))
        case ts if ts nextIf "PARTITION BY" =>
          clause = clause.copy(partitionBy = SQLTemplateParams(ts, "%F:partitionBy").fields("partitionBy"))
        case ts if ts nextIf "RANGE" =>
          clause = clause.copy(range = SQLTemplateParams(ts, "%c:condition").conditions.get("condition"))
        case ts if ts nextIf "ROW" =>
          ts.die("SELECT ... OVER ROW is not yet supported")
        case ts =>
          ts.die("Expected ORDER BY, PARTITION BY, RANGE or ROW")
      }
    }
    stream expect ")"
    clause
  }

  /**
    * Extracts a parameter list from the token stream
    * @param name the given identifier name (e.g. "customerId String, customerName String")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfParameters(name: String) = Try {
    var columns: List[Column] = Nil
    do {
      val params = SQLTemplateParams(stream, template = "%a:name %a:type ?COMMENT +?%z:comment")
      val colName = params.atoms.getOrElse("name", stream.die("Column name not provided"))
      val comment = params.atoms.get("comment").flatMap(_.noneIfBlank)
      val typeName = params.atoms.getOrElse("type", stream.die(s"Column type not provided for column $colName"))
      if (!Expression.isValidType(typeName)) stream.die(s"Invalid data type '$typeName' for column $colName")
      val column = Column(name = colName, `type` = ColumnTypes.withName(typeName.toUpperCase), comment = comment)
      columns = column :: columns
    } while (stream nextIf ",")

    SQLTemplateParams(columns = Map(name -> columns.reverse))
  }

  /**
    * Extracts the next LOCATION or TABLE from the stream
    * @param name the given identifier name (e.g. "query")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractLocationOrTable(name: String) = Try {
    val location: Location = stream match {
      case ts if ts nextIf "LOCATION" =>
        if (!ts.isQuoted) ts.die("expected a string literal representing a location path")
        LocationRef(ts.next().text)
      case ts if ts nextIf "TABLE" =>
        if (!ts.isBackticks && !ts.isText) ts.die("expected a string literal representing a table name")
        Table(ts.next().text)
      case ts if ts.isBackticks | ts.isText => Table(ts.next().text)
      case ts => ts.die("Table or location expected")
    }

    // is there an alias?
    val aliasedLocation = if (stream nextIf "AS") location.as(alias = stream.next().text) else location

    // return the results
    SQLTemplateParams(locations = Map(name -> aliasedLocation))
  }

  /**
    * Extracts the next LOCATION, TABLE or sub-query from the stream
    * @param name the given identifier name (e.g. "query")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractLocationOrTableOrSubQuery(name: String): Try[SQLTemplateParams] = stream match {
    case ts if ts is "(" => extractQueryOrVariable(name)
    case _ => extractLocationOrTable(name)
  }

  /**
    * Extracts the next instruction from the stream
    * @param name the given identifier name (e.g. "code")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractNextStatement(name: String) = Try(SQLTemplateParams(sources = Map(name -> parseNext(stream))))

  /**
    * Extracts a numeric value from the token stream
    * @param name the given identifier name (e.g. "limit")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractNumericValue(name: String) = Try {
    val text = stream.peek.map(_.text).getOrElse(stream.die(s"'$name' numeric expected"))
    Try(text.toDouble) match {
      case Success(value) =>
        stream.next()
        SQLTemplateParams(numerics = Map(name -> value))
      case Failure(_) => stream.die(s"'$name' expected a numeric value")
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
      case Failure(_) =>
        while (tags.peek.exists(_.startsWith("+?"))) tags.next()
        SQLTemplateParams()
    }
  }

  /**
    * Extracts a list of sort columns from the token stream
    * @param name the given identifier name (e.g. "sortedColumns")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractOrderedColumns(name: String) = Try {
    var sortFields: List[OrderColumn] = Nil
    do {
      // capture the column (e.g. "A.symbol" | "symbol")
      var column = stream match {
        case ts if ts.isText & ts(1).exists(_ is ".") =>
          val alias = ts.next().text
          val ascending = ts.next().text == "."
          val name = ts.next().text
          OrderColumn(name, ascending).as(alias)
        case ts if ts.isText => OrderColumn(name = ts.next().text, isAscending = true)
        case ts => ts.die("Order column definition expected")
      }

      // determine whether the column is ascending or descending
      column = column.copy(isAscending = stream match {
        case ts if ts nextIf "ASC" => true
        case ts if ts nextIf "DESC" => false
        case _ => true
      })

      // append the column to our list
      sortFields = column :: sortFields
    } while (stream nextIf ",")
    SQLTemplateParams(orderedFields = Map(name -> sortFields.reverse))
  }

  /**
    * Extracts properties from the token stream
    * @param name the given identifier name (e.g. "serdeProperties")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractProperties(name: String) = Try {

    def extractKeyOrValue(): String = {
      if (!stream.hasNext || !stream.isQuoted) throw SyntaxException("Properties: quoted value expected", stream)
      else stream.next().text
    }

    def extractKVP(): (String, String) = {
      val key = extractKeyOrValue()
      stream expect "="
      val value = extractKeyOrValue()
      key -> value
    }

    // gather the properties
    var props = Map[String, String]()
    var done = false
    stream expect "("
    do {
      done = stream is ")"
      if (!done) {
        props = props + extractKVP()
        if (stream isnt ")") stream expect ","
      }
    } while (!done)
    stream expect ")"
    SQLTemplateParams(properties = Map(name -> props))
  }

  /**
    * Parses a source expression; either a direct or via query
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractQueryOrVariable(name: String) = Try {
    val result = parseNextQueryOrVariable(stream)
    if (!result.isQuery && !result.isVariable) stream.die("Query or variable expected")
    SQLTemplateParams(sources = Map(name -> result))
  }

  /**
    * Parses a source expression; either direct (e.g. 'customers') or via query ("SELECT * FROM customers")
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractQueryTableOrVariable(name: String) = Try {
    SQLTemplateParams(sources = Map(name -> parseNextQueryTableOrVariable(stream)))
  }

  /**
    * Parses a quoted text value
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractQuotedText(name: String) = Try {
    SQLTemplateParams(atoms = Map(name -> (if (stream.isQuoted) stream.next().text else stream.die("Quoted text value expected"))))
  }

  /**
    * Parses a value based on a regular expression
    * @param pattern the regular expression pattern
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractRegEx(pattern: String) = Try {
    if (stream.matches(pattern)) SQLTemplateParams() else stream.die(s"Did not match the expected pattern '$pattern'")
  }

  /**
    * Parses repeated sequences
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractRepeatedSequence(name: String, params: SQLTemplateParams, tags: PeekableIterator[String]) = Try {
    if (!tags.nextOption.contains("{{")) stream.die("Start of sequence '{{' expected")
    else {
      // extract the repeated sequence
      val repeatedTagsSeq = tags.takeWhile(_ != "}}").toSeq
      var paramSet: List[SQLTemplateParams] = Nil
      var done = false
      while (!done && stream.hasNext) {
        var result: Try[SQLTemplateParams] = Success(SQLTemplateParams())
        val count = paramSet.size
        val repeatedTags = new PeekableIterator(repeatedTagsSeq)
        while (repeatedTags.hasNext) {
          result = processNextTag(repeatedTags.next(), params, repeatedTags)
          result.foreach(params => paramSet = params :: paramSet)
        }

        // if we didn't add anything, stop.
        done = paramSet.size == count
      }
      SQLTemplateParams(repeatedSets = Map(name -> paramSet.filterNot(_.isEmpty).reverse))
    }
  }

  /**
    * Parses storage formats
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractStorageFormat(name: String) = Try {
    var params = SQLTemplateParams()
    var done = false
    while (!done) {
      stream match {
        case ts if ts nextIf "INPUTFORMAT" => params += SQLTemplateParams(ts, s"%a:$name.input")
        case ts if ts nextIf "OUTPUTFORMAT" => params += SQLTemplateParams(ts, s"%a:$name.output")
        case _ => done = true
      }
    }
    params
  }

  private def extractTable(name: String) = Try {
    val tableName = stream match {
      case ts if ts.isBackticks | ts.isText | ts.isQuoted => ts.next().text
      case ts => ts.die("Table or view expected")
    }
    SQLTemplateParams(atoms = Map(name -> tableName))
  }

  /**
    * Parses a variable reference (e.g. "@args" or "$industry")
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractVariableReference(name: String) = Try {
    val reference = stream match {
      case ts if ts nextIf "@" => @@(ts.next().text)
      case ts if ts nextIf "$" => $(ts.next().text)
      case ts => ts.die("Variable expected")
    }
    SQLTemplateParams(variables = Map(name -> reference))
  }

  /**
    * Parses a [TABLE ...] WITH clauses (e.g. "WITH HEADERS ON")
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractWithForTableClause(name: String) = Try {
    var template = SQLTemplateParams()

    // collect the Hive/Athena style configuration properties
    var isHiveOrAthena = true
    while (isHiveOrAthena) {
      stream match {
        case ts if ts nextIf "FIELDS TERMINATED BY" => template += SQLTemplateParams(ts, "%a:delimiter")
        case ts if ts nextIf "LOCATION" => template += SQLTemplateParams(ts, "%a:path")
        case ts if ts nextIf "PARTITIONED BY" => template += SQLTemplateParams(ts, "( %P:partitions )")
        case ts if ts nextIf "ROW FORMAT DELIMITED" => template
        case ts if ts nextIf "ROW FORMAT SERDE" => template += SQLTemplateParams(ts, s"%a:serde.row")

        // input/output formats?
        case ts if ts nextIf "STORED AS" =>
          var isFormats = true
          while (isFormats) {
            ts match {
              case _ts if _ts nextIf "INPUTFORMAT" => template += SQLTemplateParams(_ts, s"%a:formats.input")
              case _ts if _ts nextIf "OUTPUTFORMAT" => template += SQLTemplateParams(_ts, s"%a:formats.output")
              case _ => isFormats = false
            }
          }

        // WITH clause?
        case ts if ts is "WITH" =>
          while (stream nextIf "WITH") {
            stream match {
              case _ts if _ts nextIf "HEADERS" => template += SQLTemplateParams(_ts, s"%C($name.headers|ON|OFF)")
              case _ts if _ts nextIf "NULL" => template += SQLTemplateParams(_ts, s"VALUES AS %a:$name.nullValue")
              case _ts if _ts nextIf "SERDEPROPERTIES" => template += SQLTemplateParams(_ts, s"%p:$name.serde")
              case _ts if _ts nextIf "TBLPROPERTIES" => template += SQLTemplateParams(_ts, s"%p:$name.table")
              case _ =>
            }
          }

        case _ => isHiveOrAthena = false
      }
    }

    template
  }

}

/**
  * SQL Template Parser Companion
  * @author lawrence.daniels@gmail.com
  */
object SQLTemplateParser {

  /**
    * Creates a new SQL Template Parser instance
    * @param ts the given [[TokenStream token stream]]
    * @return the [[SQLTemplateParser template parser]]
    */
  def apply(ts: TokenStream): SQLTemplateParser = new SQLTemplateParser(ts)

  /**
    * Invokable Enrichment
    * @param invokable the given [[Invokable]]
    */
  final implicit class InvokableEnriched(val invokable: Invokable) extends AnyVal {

    @inline def isQuery: Boolean = invokable.isInstanceOf[Queryable]

    @inline def isVariable: Boolean = invokable match {
      case _: VariableRef => true
      case _ => false
    }
  }

  /**
    * SQL Template Parser Extensions
    * @param tag the given tag
    */
  final implicit class SQLTemplateParserExtensions(val tag: String) extends AnyVal {

    /**
      * Extracts the chooser parameters (e.g. "%C(mode,INTO,OVERWRITE)" => ["mode", "INTO", "OVERWRITE"])
      */
    @inline def chooserParams: Array[String] = {
      val s = tag.drop(3).dropRight(1)
      s.indexWhere(c => !c.isLetterOrDigit && c != '.' && c != '_') match {
        case -1 => throw new IllegalArgumentException("Chooser tags require a non-alphanumeric delimiter")
        case index =>
          val delimiter = s(index)
          s.split(delimiter).map(_.trim)
      }
    }
  }

}