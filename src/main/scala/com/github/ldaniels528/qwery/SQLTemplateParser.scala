package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.{Expression, Field, OrderedColumn}
import com.github.ldaniels528.qwery.util.PeekableIterator

/**
  * SQL Template Parser
  * @author lawrence.daniels@gmail.com
  */
class SQLTemplateParser(ts: TokenStream) extends ExpressionParser {

  /**
    * Extracts the tokens that correspond to the given template
    * @param template the given template (e.g. "INSERT INTO @table ( @(fields) ) VALUES ( @[values] )")
    * @return a mapping of the extracted values
    */
  def extract(template: String): SQLTemplateParams = {
    var results = SQLTemplateParams()
    val tags = new PeekableIterator(template.split("[ ]").map(_.trim))
    while (tags.hasNext) {
      extractNextTag(tags.next(), tags) foreach (params => results = results + params)
    }
    results
  }

  private def extractNextTag(aTag: String, tags: PeekableIterator[String]): Option[SQLTemplateParams] = aTag match {
    // conditional expression? (e.g. "@&{condition}" => "x = 1 and y = 2")
    case tag if tag.startsWith("@&{") & tag.endsWith("}") => Some(extractCondition(tag.drop(3).dropRight(1)))

    // field names? (e.g. "@(fields)" => "field1, field2, ..., fieldN")
    case tag if tag.startsWith("@(") & tag.endsWith(")") => Some(extractListOfFields(tag.drop(2).dropRight(1)))

    // expressions? (e.g. "@{fields}" => "field1, 'hello', 5 + now(), ..., fieldN")
    case tag if tag.startsWith("@{") & tag.endsWith("}") => Some(extractListOfExpressions(tag.drop(2).dropRight(1)))

    // ordered field list? (e.g. "@[orderedFields]" => "field1 DESC, field2 ASC")
    case tag if tag.startsWith("@[") & tag.endsWith("]") => Some(extractSortColumns(tag.drop(2).dropRight(1)))

    // enumeration? (e.g. "@|mode|INTO|OVERWRITE|" => "INSERT INTO ..." or "INSERT OVERWRITE ...")
    case tag if tag.startsWith("@|") & tag.endsWith("|") => Some(extractEnumeratedItem(tag.drop(2).dropRight(1).split('|')))

    // regular expression match? (e.g. "@/\\d{3,4}S+/" => "123ABC")
    case tag if tag.startsWith("@/") & tag.endsWith("/") =>
      val pattern = tag.drop(2).dropRight(1)
      if (ts.matches(pattern)) None else ts.die(s"Did not match the expected pattern '$pattern'")

    // atom? (e.g. "@table" => "'./tickers.csv'")
    case tag if tag.startsWith("@") => Some(extractIdentifier(tag.drop(1)))

    // optionally required tag? (e.g. "?TOP ?@top" => "TOP 100")
    case tag if tag.startsWith("?@") => extractNextTag(tag.drop(1), tags)

    // optionally required atom? (e.g. "?ORDER +?BY ?@|sortFields|" => "ORDER BY Symbol DESC")
    case tag if tag.startsWith("+?") => ts.expect(tag.drop(2)); None

    // optional atom? (e.g. "?LIMIT ?@limit" => "LIMIT 100")
    case tag if tag.startsWith("?") => extractOptional(tag.drop(1), tags); None

    // must be literal text
    case text => ts.expect(text); None
  }

  /**
    * Extracts an enumerated item
    * @param values the values
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractEnumeratedItem(values: Seq[String]) = {
    def error[A](items: List[String]) = ts.die[A](s"One of the following '${items.mkString(", ")}' identifiers is expected")

    values.toList match {
      case name :: items =>
        val item = ts.nextOption.map(_.text).getOrElse(error(items))
        if (!items.contains(item)) error(items)
        SQLTemplateParams(atoms = Map(name -> item))
      case _ =>
        ts.die(s"Template error: ${values.mkString(", ")}")
    }
  }

  /**
    * Extracts an optional view
    * @param name the named identifier
    * @param tags the [[PeekableIterator iterator]]
    */
  private def extractOptional(name: String, tags: PeekableIterator[String]) = {
    if (!ts.nextIf(name)) {
      // if the option tag wasn't matched, skip any associated arguments
      while (tags.hasNext && tags.peek.exists(tag => tag.startsWith("?@") || tag.startsWith("+?"))) tags.next()
    }
  }

  /**
    * Extracts an identifier from the token stream
    * @param name the given identifier name (e.g. "source")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractIdentifier(name: String) = {
    val value = ts.nextOption.map(_.text).getOrElse(ts.die(s"'$name' identifier expected"))
    SQLTemplateParams(atoms = Map(name -> value))
  }

  /**
    * Extracts a field list by name from the token stream
    * @param name the given identifier name (e.g. "fields")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractListOfFields(name: String) = {
    var fields: List[Field] = Nil
    do {
      if (fields.nonEmpty) ts.expect(",")
      fields = fields ::: ts.nextOption.map(t => Field(t.text)).getOrElse(ts.die("Unexpected end of statement")) :: Nil
    } while (ts.is(","))
    SQLTemplateParams(fields = Map(name -> fields))
  }

  /**
    * Extracts a field argument list from the token stream
    * @param name the given identifier name (e.g. "customerId, COUNT(*)")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractListOfExpressions(name: String) = {

    def fetchNext(ts: TokenStream): Expression = {
      val expression = parseExpression(ts)
      val result = if (ts.nextIf("AS")) expression.map(parseNamedAlias(ts, _)) else expression
      result.getOrElse(ts.die("Unexpected end of statement"))
    }

    var expressions: List[Expression] = Nil
    do {
      if (expressions.nonEmpty) ts.expect(",")
      expressions = expressions ::: fetchNext(ts) :: Nil
    } while (ts.is(","))
    SQLTemplateParams(expressions = Map(name -> expressions))
  }

  /**
    * Extracts an expression from the token stream
    * @param name the given identifier name (e.g. "condition")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractCondition(name: String) = {
    val condition = parseCondition(ts)
    SQLTemplateParams(conditions = Map(name -> condition.getOrElse(ts.die("Conditional expression expected"))))
  }

  /**
    * Extracts a list of sort columns from the token stream
    * @param name the given identifier name (e.g. "sortedColumns")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractSortColumns(name: String) = {
    var sortFields: List[OrderedColumn] = Nil
    do {
      if (sortFields.nonEmpty) ts.expect(",")
      val name = ts.nextOption.map(_.text).getOrElse(ts.die("Unexpected end of statement"))
      val direction = ts match {
        case t if t.nextIf("ASC") => true
        case t if t.nextIf("DESC") => false
        case _ => true
      }
      sortFields = sortFields ::: OrderedColumn(name, direction) :: Nil
    } while (ts.is(","))
    SQLTemplateParams(orderedFields = Map(name -> sortFields))
  }

}

/**
  * Template Parser Companion
  * @author lawrence.daniels@gmail.com
  */
object SQLTemplateParser {

  /**
    * Creates a new TokenStream instance
    * @param query the given query string
    * @return the [[SQLTemplateParser template parser]]
    */
  def apply(query: String): SQLTemplateParser = new SQLTemplateParser(TokenStream(query))

  /**
    * Creates a new TokenStream instance
    * @param ts the given [[TokenStream token stream]]
    * @return the [[SQLTemplateParser template parser]]
    */
  def apply(ts: TokenStream): SQLTemplateParser = new SQLTemplateParser(ts)

}
