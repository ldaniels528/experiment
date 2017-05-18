package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.{Expression, Field, OrderedColumn}
import com.github.ldaniels528.qwery.util.PeekableIterator

/**
  * SQL Template Parser
  * @author lawrence.daniels@gmail.com
  */
class SQLTemplateParser(stream: TokenStream) extends ExpressionParser {

  /**
    * Extracts the tokens that correspond to the given template
    * @param template the given template (e.g. "INSERT INTO @table ( @(fields) ) VALUES ( @[values] )")
    * @return a mapping of the extracted values
    */
  def process(template: String): SQLTemplateParams = {
    var results = SQLTemplateParams()
    val tags = new PeekableIterator(template.split("[ ]").map(_.trim))
    while (tags.hasNext) {
      processNextTag(tags.next(), tags) foreach (params => results = results + params)
    }
    results
  }

  /**
    * Extracts and evaluates the next tag in the sequence
    * @param aTag the given tag (e.g. "@fields")
    * @param tags the [[PeekableIterator iteration]] of tags
    * @return the option of the resultant [[SQLTemplateParams template parameters]]
    */
  private def processNextTag(aTag: String, tags: PeekableIterator[String]): Option[SQLTemplateParams] = aTag match {
    // repeat start/end tag? (e.g. "{{values VALUES ( @{values} ) }}" => "VALUES (123, 456) VALUES (345, 678)")
    case tag if tag.startsWith("{{") => Some(processRepeatedSequence(name = tag.drop(2), tags))

    // conditional expression? (e.g. "@&{condition}" => "x = 1 and y = 2")
    case tag if tag.startsWith("@&{") & tag.endsWith("}") => Some(extractCondition(tag.drop(3).dropRight(1)))

    // field names? (e.g. "@(fields)" => "field1, field2, ..., fieldN")
    case tag if tag.startsWith("@(") & tag.endsWith(")") => Some(extractListOfFields(tag.drop(2).dropRight(1)))

    // expressions? (e.g. "@{fields}" => "field1, 'hello', 5 + now(), ..., fieldN")
    case tag if tag.startsWith("@{") & tag.endsWith("}") => Some(extractListOfExpressions(tag.drop(2).dropRight(1)))

    // ordered field list? (e.g. "@[orderedFields]" => "field1 DESC, field2 ASC")
    case tag if tag.startsWith("@[") & tag.endsWith("]") => Some(extractOrderedColumns(tag.drop(2).dropRight(1)))

    // enumeration? (e.g. "@|mode|INTO|OVERWRITE|" => "INSERT INTO ..." or "INSERT OVERWRITE ...")
    case tag if tag.startsWith("@|") & tag.endsWith("|") => Some(extractEnumeratedItem(tag.drop(2).dropRight(1).split('|')))

    // regular expression match? (e.g. "@/\\d{3,4}S+/" => "123ABC")
    case tag if tag.startsWith("@/") & tag.endsWith("/") =>
      val pattern = tag.drop(2).dropRight(1)
      if (stream.matches(pattern)) None else stream.die(s"Did not match the expected pattern '$pattern'")

    // atom? (e.g. "@table" => "'./tickers.csv'")
    case tag if tag.startsWith("@") => Some(extractIdentifier(tag.drop(1)))

    // optionally required tag? (e.g. "?TOP ?@top" => "TOP 100")
    case tag if tag.startsWith("?@") | tag.startsWith("?{{") => processNextTag(tag.drop(1), tags)

    // optionally required atom? (e.g. "?ORDER +?BY ?@|sortFields|" => "ORDER BY Symbol DESC")
    case tag if tag.startsWith("+?") => stream.expect(tag.drop(2)); None

    // optional atom? (e.g. "?LIMIT ?@limit" => "LIMIT 100")
    case tag if tag.startsWith("?") => extractOptional(tag.drop(1), tags); None

    // must be literal text
    case text => stream.expect(text); None
  }

  /**
    * Extracts an expression from the token stream
    * @param name the given identifier name (e.g. "condition")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractCondition(name: String) = {
    val condition = parseCondition(stream)
    SQLTemplateParams(conditions = Map(name -> condition.getOrElse(stream.die("Conditional expression expected"))))
  }

  /**
    * Extracts an enumerated item
    * @param values the values
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractEnumeratedItem(values: Seq[String]) = {
    def error[A](items: List[String]) = stream.die[A](s"One of the following '${items.mkString(", ")}' identifiers is expected")

    values.toList match {
      case name :: items =>
        val item = stream.nextOption.map(_.text).getOrElse(error(items))
        if (!items.contains(item)) error(items)
        SQLTemplateParams(atoms = Map(name -> item))
      case _ =>
        stream.die(s"Unexpected template error: ${values.mkString(", ")}")
    }
  }

  /**
    * Extracts an identifier from the token stream
    * @param name the given identifier name (e.g. "source")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractIdentifier(name: String) = {
    val value = stream.nextOption.map(_.text).getOrElse(stream.die(s"'$name' identifier expected"))
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
      fields = fields ::: stream.nextOption.map(t => Field(t.text)).getOrElse(stream.dieEOS) :: Nil
    } while (stream.nextIf(","))
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
      result.getOrElse(ts.dieEOS)
    }

    var expressions: List[Expression] = Nil
    do expressions = expressions ::: fetchNext(stream) :: Nil while (stream.nextIf(","))
    SQLTemplateParams(expressions = Map(name -> expressions))
  }

  /**
    * Extracts an optional view
    * @param name the named identifier
    * @param tags the [[PeekableIterator iterator]]
    */
  private def extractOptional(name: String, tags: PeekableIterator[String]) = {
    if (!stream.nextIf(name)) {
      // if the option tag wasn't matched, skip any associated arguments
      while (tags.hasNext && tags.peek.exists(tag => tag.startsWith("?@") || tag.startsWith("+?"))) tags.next()
    }
  }

  private def processRepeatedSequence(name: String, tags: PeekableIterator[String]) = {
    // extract the repeated sequence
    val repeatedTagsSeq = extractRepeatedSequence(tags)
    var paramSet: List[SQLTemplateParams] = Nil
    var done = false
    while (!done && stream.hasNext) {
      var result: Option[SQLTemplateParams] = None
      val count = paramSet.size
      val repeatedTags = new PeekableIterator(repeatedTagsSeq)
      while (repeatedTags.hasNext) {
        result = processNextTag(repeatedTags.next(), repeatedTags)
        result.foreach(params => paramSet = paramSet ::: params :: Nil)
      }

      // if we didn't add anything, stop.
      done = paramSet.size == count
    }
    SQLTemplateParams(repeatedSets = Map(name -> paramSet))
  }

  private def extractRepeatedSequence(tags: Iterator[String]) = {
    tags.takeWhile(_ != "}}").toSeq
  }

  /**
    * Extracts a list of sort columns from the token stream
    * @param name the given identifier name (e.g. "sortedColumns")
    * @return a [[SQLTemplateParams template]] represents the parsed outcome
    */
  private def extractOrderedColumns(name: String) = {
    var sortFields: List[OrderedColumn] = Nil
    do {
      val name = stream.nextOption.map(_.text).getOrElse(stream.dieEOS)
      val direction = stream match {
        case ts if ts.nextIf("ASC") => true
        case ts if ts.nextIf("DESC") => false
        case _ => true
      }
      sortFields = sortFields ::: OrderedColumn(name, direction) :: Nil
    } while (stream.nextIf(","))
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
