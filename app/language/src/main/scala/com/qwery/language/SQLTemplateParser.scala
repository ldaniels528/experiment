package com.qwery.language

import com.qwery.language.SQLTemplateParser._
import com.qwery.models.Insert.DataRow
import com.qwery.models.JoinTypes.JoinType
import com.qwery.models._
import com.qwery.models.expressions._

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

    // joins? (e.g. "%J:joins" => "INNER JOIN 'stocks.csv' ON A.symbol = B.ticker")
    case tag if tag.startsWith("%J:") => extractJoins(tag drop 3, params)

    // keyword? (e.g. "%k:LOCATION" => Set("LOCATION"))
    case tag if tag.startsWith("%k:") => extractKeyword(tag drop 3)

    // location or table? (e.g. "TABLE accounts" | "LOCATION './temp/customers/csv')
    case tag if tag.startsWith("%L") => extractLocationOrTable(tag drop 3)

    // numeric? (e.g. "%n:limit" => "100")
    case tag if tag.startsWith("%n:") => extractNumericValue(tag drop 3)

    // next statement?
    case tag if tag.startsWith("%N:") => extractNextStatement(tag drop 3)

    // ordered field list? (e.g. "%o:orderedFields" => "field1 DESC, field2 ASC")
    case tag if tag.startsWith("%o:") => extractOrderedColumns(tag drop 3)

    // parameters? (e.g. "%P:params" => "name STRING, age INTEGER, dob DATE")
    case tag if tag.startsWith("%P:") => extractListOfParameters(tag drop 3)

    // indirect query source (queries, tables and variables)? (e.g. "%q:source" => "AddressBook" | "( SELECT * FROM AddressBook )" | "@addressBook")
    case tag if tag.startsWith("%q:") => extractQueryTableOrVariable(tag drop 3)

    // direct query source (queries and variables)? (e.g. "%Q:query" => "SELECT * FROM AddressBook" | "@addressBook")
    case tag if tag.startsWith("%Q:") => extractQueryOrVariable(tag drop 3)

    // regular expression match? (e.g. "%r`\\d{3,4}\\S+`" => "123ABC")
    case tag if tag.startsWith("%r`") & tag.endsWith("`") => extractRegEx(pattern = tag.drop(3).dropRight(1))

    // repeated sequence tag? (e.g. "%R:valueSet {{ VALUES ( %E:values ) }}" => "VALUES (123, 456) VALUES (345, 678)")
    case tag if tag.startsWith("%R:") => extractRepeatSequence(name = tag drop 3, params, tags)

    // table tag? (e.g. "Customers")
    case tag if tag.startsWith("%t:") => extractTable(tag drop 3)

    // update field assignments
    case tag if tag.startsWith("%U:") => extractFieldAssignmentExpressions(tag drop 3)

    // variable reference? (e.g. "%v:variable" => "SET @variable = 5")
    case tag if tag.startsWith("%v:") => extractVariableReference(tag drop 3)

    // insert values (queries, VALUES and variables)? (e.g. "%V:data" => "(SELECT ...)" | "VALUES (...)" | "@numbers")
    case tag if tag.startsWith("%V:") => extractInsertSource(tag drop 3)

    // with clause
    case tag if tag.startsWith("%W:") => extractWithClause(tag drop 3)

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
        if (!items.exists(_.equalsIgnoreCase(item))) error(items)
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
          case ts1 if ts1 nextIf "@" => VariableRef(ts1.next().text)
          case ts1 =>
            var values: List[DataRow] = Nil
            do values = SQLTemplateParams(ts1, "( %E:values )").expressions("values") :: values while (ts1 nextIf ",")
            Insert.Values(values.reverse)
        }
      // sub-query?
      case ts if ts nextIf "(" =>
        val result = parseNext(stream)
        stream expect ")"
        result
      // variable?
      case ts if ts nextIf "@" => VariableRef(ts.next().text)
      // anything else ...
      case ts => parseNext(ts)
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
    val predicates = Seq("FULL", "JOIN", "INNER", "LEFT", "RIGHT", "OUTER")
    var joins: List[Join] = Nil

    def join(ts: TokenStream, `type`: JoinType): Join = {
      val params = SQLTemplateParams(ts, "%L:path ON %c:condition")
      Join(source = params.locations("path"), condition = params.conditions("condition"), `type` = `type`)
    }

    while (predicates.exists(stream is _)) {
      stream match {
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
    do fields = toField(stream) :: fields while (stream nextIf ",")
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
      result.getOrElse(ts.die("Unexpected end of statement"))
    }

    var expressions: List[Expression] = Nil
    do expressions = expressions ::: fetchNext(stream) :: Nil while (stream nextIf ",")
    SQLTemplateParams(expressions = Map(name -> expressions))
  }

  /**
    * Extracts a parameter list from the token stream
    * @param name the given identifier name (e.g. "customerId String, customerName String")
    * @return a [[SQLTemplateParams template]] representing the parsed outcome
    */
  private def extractListOfParameters(name: String) = Try {
    var columns: List[Column] = Nil
    do {
      val params = SQLTemplateParams(stream, template = "%a:name %a:type")
      val colName = params.atoms.getOrElse("name", stream.die("Column name not provided"))
      val typeName = params.atoms.getOrElse("type", stream.die(s"Column type not provided for column $colName"))
      if (!Expression.isValidType(typeName)) stream.die(s"Invalid data type '$typeName' for column $colName")
      val column = Column(name = colName, `type` = ColumnTypes.withName(typeName.toUpperCase))
      columns = column :: columns
    } while (stream nextIf ",")

    SQLTemplateParams(columns = Map(name -> columns.reverse))
  }

  private def extractLocationOrTable(name: String) = Try {
    val fileRef: Location = stream match {
      case ts if ts nextIf "LOCATION" =>
        if (!ts.isQuoted) ts.die("expected a string literal representing a location path")
        LocationRef(ts.next().text)
      case ts if ts nextIf "TABLE" =>
        if (!ts.isBackticks && !ts.isText) ts.die("expected a string literal representing a table name")
        TableRef.parse(ts.next().text)
      case ts if ts.isBackticks | ts.isText => TableRef.parse(ts.next().text)
      case ts => ts.die("Table or location expected")
    }

    // is there an alias?
    val fileRefWithAlias = if (stream nextIf "AS") fileRef.as(alias = stream.next().text) else fileRef
    SQLTemplateParams(locations = Map(name -> fileRefWithAlias))
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
        case ts if ts.isText => OrderColumn(name = ts.next().text)
        case ts => ts.die("Order column definition expected")
      }

      // determine whether the column is ascending or descending
      column = column.copy(ascending = stream match {
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
    * Parses a source expression; either a direct or via query
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractQueryOrVariable(name: String) = Try {
    val result = parseNextQueryTableOrVariable(stream)
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

  private def extractRegEx(pattern: String) = Try {
    if (stream.matches(pattern)) SQLTemplateParams() else stream.die(s"Did not match the expected pattern '$pattern'")
  }

  private def extractRepeatSequence(name: String, params: SQLTemplateParams, tags: PeekableIterator[String]) = Try {
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

  private def extractTable(name: String) = Try {
    val tableName = stream match {
      case ts if ts.isBackticks | ts.isText | ts.isQuoted => ts.next().text
      case ts => ts.die("Table or view expected")
    }
    SQLTemplateParams(atoms = Map(name -> tableName))
  }

  /**
    * Parses a variable reference (e.g. "@args")
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractVariableReference(name: String) = Try {
    val reference = stream match {
      case ts if ts nextIf "@" => VariableRef(ts.next().text)
      case ts => ts.die("Variable expected")
    }
    SQLTemplateParams(variables = Map(name -> reference))
  }

  /**
    * Parses a WITH clauses (e.g. "WITH ARGUMENTS AS @args")
    * @param name the named identifier
    * @return the [[SQLTemplateParams SQL template parameters]]
    */
  private def extractWithClause(name: String) = Try {
    val mapping = scala.collection.mutable.Map[String, String]()
    var template = SQLTemplateParams()
    while (stream nextIf "WITH") {
      stream match {
        case ts if ts is "ARGUMENTS" => template += SQLTemplateParams(ts, "ARGUMENTS AS %v:name")
        case ts if ts is "ENVIRONMENT" => template += SQLTemplateParams(ts, "ENVIRONMENT AS %v:name")
        case ts if ts nextIf "HIVE SUPPORT" => template += SQLTemplateParams(atoms = Map("hiveSupport" -> "true"))
        case ts if ts(1).exists(_ is "PROCESSING") => template += SQLTemplateParams(ts, "%C(processing|BATCH|STREAM) PROCESSING")
        case ts =>
          ts.die("Invalid WITH expression")
      }
    }
    template
  }

  /**
    * Creates a new field from a token
    * @param tokenStream the given [[TokenStream token stream]]
    * @return a new [[Field field]] instance
    */
  private def toField(tokenStream: TokenStream): Field = tokenStream match {
    case ts if ts nextIf "*" => AllFields
    case ts => Field(ts.next().text)
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
    * SQL Template Parser Extensions
    * @param tag the given tag
    */
  final implicit class SQLTemplateParserExtensions(val tag: String) extends AnyVal {

    /**
      * Extracts the chooser parameters (e.g. "%C(mode,INTO,OVERWRITE)" => ["mode", "INTO", "OVERWRITE"])
      */
    @inline def chooserParams: Array[String] = {
      val s = tag.drop(3).dropRight(1)
      s.indexWhere(!_.isLetterOrDigit) match {
        case -1 => throw new IllegalArgumentException("Chooser tags require a non-alphanumeric delimiter")
        case index =>
          val delimiter = s(index)
          s.split(delimiter).map(_.trim)
      }
    }
  }

}