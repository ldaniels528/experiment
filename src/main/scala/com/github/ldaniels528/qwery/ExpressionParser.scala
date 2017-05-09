package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ops._

/**
  * Expression Parser
  * @author lawrence.daniels@gmail.com
  */
trait ExpressionParser {

  def parseExpressions(tok: TokenStream): Option[Expression] = {
    var expression: Option[Expression] = None
    var done: Boolean = false
    while (tok.hasNext && !done) {
      // continued expression? ... AND field = 1
      if (expression.nonEmpty) {
        tok.next() match {
          case AlphaToken(opr, _) if opr.equalsIgnoreCase("AND") =>
            expression = expression.map(expr => AND(expr, parseExpression(tok)))
          case AlphaToken(opr, _) if opr.equalsIgnoreCase("OR") =>
            expression = expression.map(expr => OR(expr, parseExpression(tok)))
          case _ =>
            done = true
            tok.previous
        }
      }

      else {
        expression = Option(parseExpression(tok))
      }
    }
    expression
  }

  private def parseExpression(tok: TokenStream): Expression = {
    tok.take(3).toList match {
      case List(AlphaToken(name, _), opt@OperatorToken(operator, _), token) =>
        operator match {
          case "=" => EQ(Field(name), Evaluatable(token.value))
          case ">" => GT(Field(name), Evaluatable(token.value))
          case ">=" => GE(Field(name), Evaluatable(token.value))
          case "<" => LT(Field(name), Evaluatable(token.value))
          case "<=" => LE(Field(name), Evaluatable(token.value))
          case "!=" => NE(Field(name), Evaluatable(token.value))
          case "<>" => NE(Field(name), Evaluatable(token.value))
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
