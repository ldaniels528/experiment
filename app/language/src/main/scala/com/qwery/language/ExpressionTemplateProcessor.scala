package com.qwery.language

import com.qwery.models._
import com.qwery.models.expressions.{Expression, Field}

import scala.util.{Failure, Success, Try}

/**
  * Expression Template Processor
  * @author lawrence.daniels@gmail.com
  */
trait ExpressionTemplateProcessor {

  /**
    * Parses the given template expression
    * @param template the given template expression (e.g. "%a:name")
    * @param ts       the given [[TokenStream]]
    * @param parser   the implicit [[ExpressionParser expression parser]]
    * @return the [[ExpressionTemplate]]
    */
  def process(template: String, ts: TokenStream)(implicit parser: ExpressionParser): ExpressionTemplate = {
    val tags = template.split("[ ]")
    tags.foldLeft[ExpressionTemplate](ExpressionTemplate()) {
      // atom?
      case (result, tag) if tag.startsWith("%a:") => result + extractAtom(name = tag.drop(3), ts)
      // condition?
      case (result, tag) if tag.startsWith("%c:") => result + extractCondition(name = tag.drop(3), ts)
      // expression?
      case (result, tag) if tag.startsWith("%e:") => result + extractExpression(name = tag.drop(3), ts)
      // expression list?
      case (result, tag) if tag.startsWith("%E:") => result + extractExpressionList(name = tag.drop(3), ts)
      // field?
      case (result, tag) if tag.startsWith("%f:") => result + extractField(name = tag.drop(3), ts)
      // type?
      case (result, tag) if tag.startsWith("%t:") => result + extractType(name = tag.drop(3), ts)
      // variable?
      case (result, tag) if tag.startsWith("%v:") => result + extractVariable(name = tag.drop(3), ts)
      // must be a keyword/symbol tag
      case (result, tag) => ts.expect(tag); result
    }
  }

  /**
    * Upon successful processing of the given template expression, the state is updated and a value is returned.
    * @param template the given template expression (e.g. "%a:name")
    * @param ts       the given [[TokenStream]]
    * @param parser   the implicit [[ExpressionParser expression parser]]
    * @return the option of the [[ExpressionTemplate]]
    */
  def processOpt(template: String, ts: TokenStream)(implicit parser: ExpressionParser): Option[ExpressionTemplate] = {
    ts.mark()
    Try(process(template, ts)) match {
      case Success(result) =>
        ts.discard()
        Option(result)
      case Failure(_) =>
        ts.reset()
        None
    }
  }

  /**
    * Parses an atom (e.g. keyword, etc.)
    * @param name the name of the atom reference
    * @param ts   the given [[TokenStream token stream]]
    * @return the [[ExpressionTemplate]]
    */
  private def extractAtom(name: String, ts: TokenStream): ExpressionTemplate = {
    val atom = ts.next().text
    ExpressionTemplate(atoms = Map(name -> atom))
  }

  /**
    * Parses a condition
    * @param name the name of the condition
    * @param ts   the given [[TokenStream token stream]]
    * @return the [[ExpressionTemplate]]
    */
  private def extractCondition(name: String, ts: TokenStream)(implicit parser: ExpressionParser): ExpressionTemplate = {
    val condition = parser.parseCondition(ts).getOrElse(ts.die("Condition expected"))
    ExpressionTemplate(conditions = Map(name -> condition))
  }

  /**
    * Parses an expression
    * @param name the name of the expression
    * @param ts   the given [[TokenStream token stream]]
    * @return the [[ExpressionTemplate]]
    */
  private def extractExpression(name: String, ts: TokenStream)(implicit parser: ExpressionParser): ExpressionTemplate = {
    val expression = parser.parseExpression(ts).getOrElse(ts.die("Expression expected"))
    ExpressionTemplate(expressions = Map(name -> expression))
  }

  /**
    * Parses an expression list
    * @param name the name of the expression list
    * @param ts   the given [[TokenStream token stream]]
    * @return the [[ExpressionTemplate]]
    */
  private def extractExpressionList(name: String, ts: TokenStream)(implicit parser: ExpressionParser): ExpressionTemplate = {
    var list: List[Expression] = Nil
    do {
      parser.parseExpression(ts) foreach (expr => list = expr :: list)
    } while (ts nextIf ",")
    ExpressionTemplate(expressionLists = Map(name -> list.reverse))
  }

  /**
    * Parses a field reference
    * @param name   the name of the field reference
    * @param stream the given [[TokenStream token stream]]
    * @return the [[ExpressionTemplate]]
    */
  private def extractField(name: String, stream: TokenStream): ExpressionTemplate = {
    val field = stream.next() match {
      case t: AlphaToken => Field(t.text)
      case t: QuotedToken if t.isBackTicks => Field(t.text)
      case t =>
        throw new IllegalArgumentException(s"Token '$t' is not valid (type: ${t.getClass.getName})")
    }
    ExpressionTemplate(fields = Map(name -> field))
  }

  /**
    * Parses a type reference
    * @param name the name of the type reference
    * @param ts   the given [[TokenStream token stream]]
    * @return the [[ExpressionTemplate]]
    */
  private def extractType(name: String, ts: TokenStream): ExpressionTemplate = {
    val `type` = ColumnTypes.withName(ts.next().text.toUpperCase())
    ExpressionTemplate(types = Map(name -> `type`))
  }

  /**
    * Parses a variable reference
    * @param name   the name of the variable reference
    * @param stream the given [[TokenStream token stream]]
    * @return the [[ExpressionTemplate]]
    */
  private def extractVariable(name: String, stream: TokenStream): ExpressionTemplate = {
    val variable = stream match {
      case ts if ts nextIf "@" => @@(ts.next().text)
      case ts if ts nextIf "$" => $(ts.next().text)
      case ts => ts.die("Variable expected")
    }
    ExpressionTemplate(variables = Map(name -> variable))
  }

}
