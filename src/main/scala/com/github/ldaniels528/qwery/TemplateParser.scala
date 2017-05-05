package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.TemplateParser.TokenStreamExtensions
import com.github.ldaniels528.qwery.util.PeekableIterator

/**
  * Template Parser
  * @author lawrence.daniels@gmail.com
  */
class TemplateParser(ts: TokenStream) extends ExpressionParser {

  /**
    * Extracts the tokens that correspond to the given template
    * @example
    * {{{
    *   SELECT @fields FROM @source WHERE @<conditions>
    * }}}
    * @example
    * {{{
    *   INSERT INTO @table @(fields) VALUES @[values]
    * }}}
    * @param template the given template (e.g. "INSERT INTO $table @(fields) VALUES #(values)")
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

        // field parameter list? (e.g. "@(fields)" => "(field1, field2, ..., fieldN)")
        case tag if tag.startsWith("@(") & tag.endsWith(")") =>
          results = results + extractFieldParameters(tag.drop(2).dropRight(1))

        // insert values list? (e.g. "@[values]" => "('hello', 'world', ..., 1234)")
        case tag if tag.startsWith("@[") & tag.endsWith("]") =>
          results = results + extractValueList(tag.drop(2).dropRight(1))

        // field argument list? (e.g. "@{fields}" => "field1, field2 + 1, ..., fieldN")
        case tag if tag.startsWith("@{") & tag.endsWith("}") =>
          results = results + extractFieldArguments(tag.drop(2).dropRight(1))

        // regular expression match? (e.g. "@/\\d{3,4}S+/" => "123ABC")
        case tag if tag.startsWith("@/") & tag.endsWith("/") =>
          val pattern = tag.drop(2).dropRight(1)
          if (ts.matches(pattern)) die(s"Did not match the expected pattern '$pattern'")

        // single string argument? (e.g. "@customer" => "Larry Davis")
        case tag if tag.startsWith("@") => results = results + extractArgument(tag.drop(1))

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
      while (tags.hasNext && tags.peek.exists(_.startsWith("@"))) tags.next()
    }
  }

  /**
    * Extracts a single string argument from the token stream
    * @param name the given variable name (e.g. "source")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractArgument(name: String) = {
    val value = ts.nextOption.map(_.text).getOrElse(die(s"'$name' value expected"))
    Template(arguments = Map(name -> value))
  }

  /**
    * Extracts a field argument list from the token stream
    * @param name the given variable name (e.g. "fields")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractFieldArguments(name: String) = {
    var fields: List[Field] = Nil
    do {
      if (fields.nonEmpty) ts.expect(",")
      fields = ts.nextOption.map(t => Field(t.text)).getOrElse(die("Unexpected end of statement")) :: fields
    } while (ts.is(","))
    Template(fields = Map(name -> fields.reverse))
  }

  /**
    * Extracts a field parameter list from the token stream
    * @param name the given variable name (e.g. "fields")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractFieldParameters(name: String) = {
    var fields: List[Field] = Nil
    ts.expect("(")
    while (!ts.is(")")) {
      if (fields.nonEmpty) ts.expect(",")
      fields = ts.nextOption.map(t => Field(t.text)).getOrElse(die("Unexpected end of statement")) :: fields
    }
    ts.expect(")")
    Template(fields = Map(name -> fields.reverse))
  }

  /**
    * Extracts an expression from the token stream
    * @param name the given variable name (e.g. "condition")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractExpression(name: String) = {
    val expression = parseExpressions(ts)
    Template(expressions = Map(name -> expression.getOrElse(die("Expression expected"))))
  }

  /**
    * Extracts a value list from the token stream
    * @param name tthe given variable name (e.g. "values")
    * @return a [[Template template]] represents the parsed outcome
    */
  private def extractValueList(name: String) = {
    var values: List[Any] = Nil
    ts.expect("(")
    while (!ts.is(")")) {
      if (values.nonEmpty) ts.expect(",")
      values = ts.nextOption.map(_.value).getOrElse(die("Unexpected end of statement")) :: values
    }
    ts.expect(")")
    Template(values = Map(name -> values.reverse))
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
    * @param ts the given [[TokenStream token stream]]
    * @return the [[TemplateParser template parser]]
    */
  def apply(ts: TokenStream): TemplateParser = new TemplateParser(ts)

  /**
    * Creates a new TokenStream instance
    * @param query the given query string
    * @return the [[TemplateParser template parser]]
    */
  def apply(query: String): TemplateParser = new TemplateParser(TokenStream(query))

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
