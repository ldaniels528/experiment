package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.Field.AllFields
import com.github.ldaniels528.qwery.TemplateParser.TokenStreamExtensions
import com.github.ldaniels528.qwery.util.PeekableIterator

/**
  * Template Parser
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
        // conditional expression? e.g. "@<condition>" => "x = 1 and y = 2"
        case tag if tag.startsWith("@<") & tag.endsWith(">") =>
          results = results + extractExpression(tag.drop(2).dropRight(1))

        // field name list? (e.g. "@(fields)" => "field1, field2, ..., fieldN")
        case tag if tag.startsWith("@(") & tag.endsWith(")") =>
          results = results + extractFieldListByName(tag.drop(2).dropRight(1))

        // field arguments list? (e.g. "@{fields}" => "field1, 'hello', 5 + now(), ..., fieldN")
        case tag if tag.startsWith("@{") & tag.endsWith("}") =>
          results = results + extractFieldArgumentList(tag.drop(2).dropRight(1))

        // insert values list? (e.g. "@[values]" => "('hello', 'world', ..., 1234)")
        case tag if tag.startsWith("@[") & tag.endsWith("]") =>
          results = results + extractValueList(tag.drop(2).dropRight(1))

        // sort field list? (e.g. "@|sortFields|" => "field1 DESC, field2 ASC")
        case tag if tag.startsWith("@|") & tag.endsWith("|") =>
          results = results + extractSortFields(tag.drop(2).dropRight(1))

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
    val value = ts.nextOption.map(_.text).getOrElse(die(s"'$name' value expected"))
    Template(identifiers = Map(name -> value))
  }

  /**
    * Extracts a field list by name from the token stream
    * @param name the given identifier name (e.g. "fields")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractFieldListByName(name: String) = {
    var fields: List[Field] = Nil
    do {
      if (fields.nonEmpty) ts.expect(",")
      fields = fields ::: ts.nextOption.map(t => Field(t.text)).getOrElse(die("Unexpected end of statement")) :: Nil
    } while (ts.is(","))
    Template(fieldReferences = Map(name -> fields))
  }

  /**
    * Extracts a field argument list from the token stream
    * @param name the given identifier name (e.g. "fields")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractFieldArgumentList(name: String) = {
    var arguments: List[Evaluatable] = Nil
    do {
      if (arguments.nonEmpty) ts.expect(",")
      arguments = arguments ::: ts.nextOption.map {
        case t if t.text == "*" => AllFields
        case t: AlphaToken => Field(t.text)
        case t: QuotedToken if t.isBackticks => Field(t.text)
        case t => Evaluatable(t)
      }.getOrElse(die("Unexpected end of statement")) :: Nil
    } while (ts.is(","))
    Template(fieldArguments = Map(name -> arguments))
  }

  /**
    * Extracts an expression from the token stream
    * @param name the given identifier name (e.g. "condition")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractExpression(name: String) = {
    val expression = parseExpressions(ts)
    Template(expressions = Map(name -> expression.getOrElse(die("Expression expected"))))
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

  /**
    * Extracts a value list from the token stream
    * @param name the given identifier name (e.g. "values")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractValueList(name: String) = {
    var values: List[Any] = Nil
    while (!ts.is(")")) {
      if (values.nonEmpty) ts.expect(",")
      values = values ::: ts.nextOption.map(_.value).getOrElse(die("Unexpected end of statement")) :: Nil
    }
    Template(insertValues = Map(name -> values))
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
