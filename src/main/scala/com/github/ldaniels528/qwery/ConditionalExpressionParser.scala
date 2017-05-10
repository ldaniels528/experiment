package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops._

/**
  * Conditional Expression Parser
  * @author lawrence.daniels@gmail.com
  */
trait ConditionalExpressionParser {

  def parseConditions(ts: TokenStream): Option[Expression] = {
    var expression: Option[Expression] = None
    var done: Boolean = false
    while (ts.hasNext && !done) {
      // continued expression? ... AND field = 1
      if (expression.nonEmpty) {
        ts.next() match {
          case AlphaToken(opr, _) if opr.equalsIgnoreCase("AND") =>
            expression = expression.map(expr => AND(expr, parseCondition(ts)))
          case AlphaToken(opr, _) if opr.equalsIgnoreCase("OR") =>
            expression = expression.map(expr => OR(expr, parseCondition(ts)))
          case _ =>
            done = true
            ts.previous
        }
      }

      else {
        expression = Option(parseCondition(ts))
      }
    }
    expression
  }

  private def parseCondition(ts: TokenStream): Expression = {
    ts.take(3).toList match {
      case List(AlphaToken(name, _), opt@OperatorToken(operator, _), token) =>
        operator match {
          case "=" => EQ(Field(name), Value(token.value))
          case ">" => GT(Field(name), Value(token.value))
          case ">=" => GE(Field(name), Value(token.value))
          case "<" => LT(Field(name), Value(token.value))
          case "<=" => LE(Field(name), Value(token.value))
          case "!=" => NE(Field(name), Value(token.value))
          case "<>" => NE(Field(name), Value(token.value))
          case _ =>
            throw new SyntaxException(s"Invalid operator", token)
        }
      case List(ft, opt@AlphaToken(operator, _), QuotedToken(matching, _, _)) =>
        operator match {
          case "LIKE" => LIKE(Field(ft.text), matching)
          case _ =>
            throw new SyntaxException(s"Invalid operator", opt)
        }
      case tokens =>
        throw new SyntaxException(s"Expected expression", tokens.headOption.orNull)
    }
  }

}
