package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops.{Expression, Field}
import com.github.ldaniels528.qwery.util.PeekableIterator

/**
  * SQL Template Parser
  * @author lawrence.daniels@gmail.com
  */
class TemplateParser(ts: TokenStream) extends ExpressionParser {

  /**
    * Extracts the tokens that correspond to the given template
    * @param template the given template (e.g. "INSERT INTO @table ( @(fields) ) VALUES ( @[values] )")
    * @return a mapping of the extracted values
    */
  def extract(template: String): Template = {
    var results = Template()
    val tags = new PeekableIterator(template.split("[ ]").map(_.trim))
    while (tags.hasNext) {
      tags.next() match {
        // conditional expression? (e.g. "@<condition>" => "x = 1 and y = 2")
        case tag if tag.startsWith("@<") & tag.endsWith(">") =>
          results = results + extractCondition(tag.drop(2).dropRight(1))

        // field names? (e.g. "@(fields)" => "field1, field2, ..., fieldN")
        case tag if tag.startsWith("@(") & tag.endsWith(")") =>
          results = results + extractListOfFields(tag.drop(2).dropRight(1))

        // expressions? (e.g. "@{fields}" => "field1, 'hello', 5 + now(), ..., fieldN")
        case tag if tag.startsWith("@{") & tag.endsWith("}") =>
          results = results + extractListOfExpressions(tag.drop(2).dropRight(1))

        // sort field list? (e.g. "@[sortFields]" => "field1 DESC, field2 ASC")
        case tag if tag.startsWith("@[") & tag.endsWith("]") =>
          results = results + extractSortFields(tag.drop(2).dropRight(1))

        // enumeration? (e.g. "@|mode|INTO|OVERWRITE|" => "INSERT INTO ..." or "INSERT OVERWRITE ...")
        case tag if tag.startsWith("@|") & tag.endsWith("|") =>
          results = results + extractEnumeratedItem(tag.drop(2).dropRight(1).split('|'))

        // regular expression match? (e.g. "@/\\d{3,4}S+/" => "123ABC")
        case tag if tag.startsWith("@/") & tag.endsWith("/") =>
          val pattern = tag.drop(2).dropRight(1)
          if (ts.matches(pattern)) die(s"Did not match the expected pattern '$pattern'")

        // identifier? (e.g. "@table" => "'./tickers.csv'")
        case tag if tag.startsWith("@") => results = results + extractIdentifier(tag.drop(1))

        // optional dependent-identifier? (e.g. "?ORDER +?BY @|sortFields|" => "ORDER BY Symbol DESC")
        case tag if tag.startsWith("+?") => ts.expect(tag.drop(2))

        // optional identifier? (e.g. "?LIMIT @limit" => "LIMIT 100")
        case tag if tag.startsWith("?") => extractOptional(tag.drop(1), tags)

        // literal text?
        case text => ts.expect(text)
      }
    }
    results
  }

  /**
    * Extracts an enumerated item
    * @param values the values
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractEnumeratedItem(values: Seq[String]) = {
    def error[A](items: List[String]) = die[A](s"One of the following '${items.mkString(", ")}' identifiers is expected")

    values.toList match {
      case name :: items =>
        val item = ts.nextOption.map(_.text).getOrElse(error(items))
        if(!items.contains(item)) error(items)
        Template(identifiers = Map(name -> item))
      case _ =>
        die(s"Template error: ${values.mkString(", ")}")
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
      while (tags.hasNext && (tags.peek.exists(_.startsWith("@")) || tags.peek.exists(_.startsWith("+?")))) tags.next()
    }
  }

  /**
    * Extracts an identifier from the token stream 
    * @param name the given identifier name (e.g. "source")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractIdentifier(name: String) = {
    val value = ts.nextOption.map(_.text).getOrElse(die(s"'$name' identifier expected"))
    Template(identifiers = Map(name -> value))
  }

  /**
    * Extracts a field list by name from the token stream
    * @param name the given identifier name (e.g. "fields")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractListOfFields(name: String) = {
    var fields: List[Field] = Nil
    do {
      if (fields.nonEmpty) ts.expect(",")
      fields = fields ::: ts.nextOption.map(t => Field(t.text)).getOrElse(die("Unexpected end of statement")) :: Nil
    } while (ts.is(","))
    Template(fields = Map(name -> fields))
  }

  /**
    * Extracts a field argument list from the token stream
    * @param name the given identifier name (e.g. "customerId, COUNT(*)")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractListOfExpressions(name: String) = {
    var expressions: List[Expression] = Nil
    do {
      if (expressions.nonEmpty) ts.expect(",")
      expressions = expressions ::: parseExpression(ts).getOrElse(die("Unexpected end of statement")) :: Nil
    } while (ts.is(","))
    Template(expressions = Map(name -> expressions))
  }

  /**
    * Extracts an expression from the token stream
    * @param name the given identifier name (e.g. "condition")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractCondition(name: String) = {
    val condition = parseCondition(ts)
    Template(conditions = Map(name -> condition.getOrElse(die("Conditional expression expected"))))
  }

  /**
    * Extracts a value list from the token stream
    * @param name the given identifier name (e.g. "values")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractSortFields(name: String) = {
    var sortFields: List[(Field, Int)] = Nil
    do {
      if (sortFields.nonEmpty) ts.expect(",")
      val field = ts.nextOption.map(t => Field(t.text)).getOrElse(die("Unexpected end of statement"))
      val direction = ts match {
        case t if t.is("ASC") => ts.next(); 1
        case t if t.is("DESC") => ts.next(); -1
        case _ => 1
      }
      sortFields = sortFields ::: field -> direction :: Nil
    } while (ts.is(","))
    Template(sortFields = Map(name -> sortFields))
  }

  private def die[A](message: String): A = throw new SyntaxException(message, ts.peek.orNull)

}

/**
  * Template Parser Companion
  * @author lawrence.daniels@gmail.com
  */
object TemplateParser {

  /**
    * Creates a new TokenStream instance
    * @param query the given query string
    * @return the [[TemplateParser template parser]]
    */
  def apply(query: String): TemplateParser = new TemplateParser(TokenStream(query))

  /**
    * Creates a new TokenStream instance
    * @param ts the given [[TokenStream token stream]]
    * @return the [[TemplateParser template parser]]
    */
  def apply(ts: TokenStream): TemplateParser = new TemplateParser(ts)

  /**
    * Token Stream Extensions
    * @param ts the given [[TokenStream token stream]]
    */
  implicit class TokenStreamExtensions(val ts: TokenStream) extends AnyVal {

    @inline
    def expect(keyword: String): Unit = {
      if (!ts.nextIf(keyword)) throw new SyntaxException(s"Keyword '$keyword' expected", ts.peek.orNull)
    }

  }

}
