package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ExpressionParser._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.math._
import com.github.ldaniels528.qwery.ops.builtins._

/**
  * Expression Parser
  * @author lawrence.daniels@gmail.com
  */
trait ExpressionParser {

  def parseCondition(stream: TokenStream): Option[Condition] = {
    var condition: Option[Condition] = None
    var done: Boolean = false
    do {
      if (condition.isEmpty) condition = parseNextCondition(stream)
      else {
        val newCondition = stream match {
          case ts if ts.nextIf("AND") => for (a <- condition; b <- parseNextCondition(ts)) yield AND(a, b)
          case ts if ts.nextIf("OR") => for (a <- condition; b <- parseNextCondition(ts)) yield OR(a, b)
          case _ => None
        }
        if (newCondition.nonEmpty) condition = newCondition else done = true
      }
    } while (!done && condition.nonEmpty && stream.hasNext)
    condition
  }

  def parseExpression(stream: TokenStream): Option[Expression] = {
    var expression: Option[Expression] = None
    var done: Boolean = false
    do {
      if (expression.isEmpty) expression = parseNextExpression(stream)
      else {
        val result = stream match {
          case ts if ts.nextIf("+") => for (a <- expression; b <- parseNextExpression(ts)) yield a + b
          case ts if ts.nextIf("-") => for (a <- expression; b <- parseNextExpression(ts)) yield a - b
          case ts if ts.nextIf("*") => for (a <- expression; b <- parseNextExpression(ts)) yield a * b
          case ts if ts.nextIf("/") => for (a <- expression; b <- parseNextExpression(ts)) yield a / b
          case ts if ts.nextIf("|") => for (a <- expression; b <- parseNextExpression(ts)) yield a | b
          case _ => None
        }
        // if the expression was resolved ...
        if (result.nonEmpty) expression = result else done = true
      }
    } while (!done && expression.nonEmpty && stream.hasNext)
    expression
  }

  /**
    * Parses an internal function (e.g. "LEN('Hello World')")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[InternalFunction internal function]]
    */
  private def parseInternalFunction(stream: TokenStream): Option[InternalFunction] = {
    stream match {
      // is it a parameter-less function? (e.g. "Now()")
      case ts if function0s.exists(fx => ts.is(fx.name)) =>
        function0s.find(f => ts.nextIf(f.name)) map { fx =>
          ts.expect("(").expect(")")
          fx
        }
      // is it a single-parameter function? (e.g. "Trim('Hello ')")
      case ts if function1s.exists { case (name, _) => ts.is(name) } =>
        function1s.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
          ts.expect("(")
          val a = parseExpression(ts).getOrElse(invalidParameters(ts, name, 1))
          ts.expect(")")
          fx(a)
        }
      // is it a two-parameter function? (e.g. "Left('Hello World', 6)")
      case ts if function2s.exists { case (name, _) => ts.is(name) } =>
        function2s.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
          ts.expect("(")
          val a = parseExpression(ts).getOrElse(invalidParameters(ts, name, 2))
          ts.expect(",")
          val b = parseExpression(ts).getOrElse(invalidParameters(ts, name, 2))
          ts.expect(")")
          fx(a, b)
        }
      // is it a three-parameter function? (e.g. "SubString('Hello World', 6, 5)")
      case ts if function3s.exists { case (name, _) => ts.is(name) } =>
        function3s.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
          ts.expect("(")
          val a = parseExpression(ts).getOrElse(invalidParameters(ts, name, 3))
          ts.expect(",")
          val b = parseExpression(ts).getOrElse(invalidParameters(ts, name, 3))
          ts.expect(",")
          val c = parseExpression(ts).getOrElse(invalidParameters(ts, name, 3))
          ts.expect(")")
          fx(a, b, c)
        }
      case ts => ts.die(s"${ts.peek.orNull} is not a defined function")
    }
  }

  private def invalidParameters(ts: TokenStream, name: String, expected: Int) = {
    expected match {
      case 0 =>
        ts.die(s"Function $name expects no parameters")
      case 1 =>
        ts.die(s"Function $name expects a single parameter")
      case n =>
        ts.die(s"Function $name expects $n parameters")
    }
  }

  private def parseNextCondition(stream: TokenStream): Option[Condition] = {
    stream match {
      case ts if ts.nextIf("NOT") => parseNOT(ts)
      case ts =>
        var condition: Option[Condition] = None
        var expression: Option[Expression] = None
        var done: Boolean = false

        do {
          if (expression.isEmpty) expression = parseExpression(ts)
          else {
            val result = for {
              (_, op) <- conditionalOps.find { case (symbol, _) => ts.nextIf(symbol) }
              a <- expression
              b <- parseExpression(ts)
            } yield op(a, b)
            if (result.nonEmpty) condition = result else done = true
          }
        } while (!done && expression.nonEmpty && ts.hasNext)
        condition
    }
  }

  private def parseNextExpression(stream: TokenStream): Option[Expression] = {
    stream match {
      // is it a special function?
      case ts if ts.nextIf("Cast") => parseCast(ts)
      // is it an all fields reference?
      case ts if ts.nextIf("*") => Option(AllFields)
      // is it a quantity (e.g. "(2 + (5 * 2))")?
      case ts if ts.nextIf("(") =>
        val expr = parseExpression(ts)
        ts.expect(")")
        expr
      // is it a function?
      case ts if ts.matches(identifierRegEx) & ts.peekAhead(1).exists(_.is("(")) => parseInternalFunction(stream)
      // is it a field or constant value?
      case ts if ts.isNumeric | ts.isQuoted => Option(Expression(ts.next()))
      case ts if ts.matches(identifierRegEx) | ts.isBackticks => Option(Field(ts.next()))
      case _ => None
    }
  }

  /**
    * Parses an expression alias (e.g. "(1 + 3) * 2 AS qty")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression CAST expression]]
    */
  def parseNamedAlias(stream: TokenStream, expression: Expression): NamedExpression = {
    stream match {
      case ts if ts.isBackticks | ts.matches(identifierRegEx) => NamedExpression.alias(name = ts.next().text, expression)
      case ts => ts.die("Identifier expected for alias")
    }
  }

  /**
    * Parses a CAST expression (e.g. "CAST(1234 as String)")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Expression CAST expression]]
    */
  private def parseCast(ts: TokenStream): Option[Cast] = {
    ts.expect("(")
    val expression = parseExpression(ts) map { value =>
      ts.expect("AS")
      val toType = ts.nextOption.map(_.text).getOrElse(ts.die("Type expected"))
      ts.expect(")")
      Cast(value, toType)
    }
    expression getOrElse ts.die("Syntax error")
    expression
  }

  /**
    * Parses a NOT condition (e.g. "NOT X = 1")
    * @param ts the given [[TokenStream token stream]]
    * @return a [[Condition condition]]
    */
  private def parseNOT(ts: TokenStream): Option[NOT] = {
    val condition = parseCondition(ts)
      .getOrElse(ts.die("Conditional expression expected"))
    Option(NOT(condition))
  }

}

/**
  * Expression Parser Singleton
  * @author lawrence.daniels@gmail.com
  */
object ExpressionParser {
  private val identifierRegEx = "[_a-zA-Z][_a-zA-Z0-9]{0,30}"
  private val function0s = Seq(
    Now
  )
  private val function1s = Map(
    "AVG" -> Avg.apply _,
    "COUNT" -> Count.apply _,
    "LEN" -> Len.apply _,
    "MAX" -> Max.apply _,
    "MIN" -> Min.apply _,
    "SQRT" -> Sqrt.apply _,
    "SUM" -> Sum.apply _,
    "TRIM" -> Trim.apply _
  )
  private val function2s = Map(
    "LEFT" -> Left.apply _,
    "RIGHT" -> Right.apply _,
    "SPLIT" -> Split.apply _
  )
  private val function3s = Map(
    "SUBSTRING" -> Substring.apply _
  )
  private val conditionalOps = Map(
    "=" -> EQ.apply _,
    ">=" -> GE.apply _,
    ">" -> GT.apply _,
    "<=" -> LE.apply _,
    "<" -> LT.apply _,
    "<>" -> NE.apply _,
    "!=" -> NE.apply _,
    "LIKE" -> LIKE.apply _
  )

  /**
    * Indicates whether the given name qualifies as an identifier (e.g. "customerName")
    * @param name the given name
    * @return true, if the given name qualifies as an identifier
    */
  def isIdentifier(name: String): Boolean = name.matches(identifierRegEx)

}