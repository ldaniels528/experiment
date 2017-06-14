package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ExpressionParser._
import com.github.ldaniels528.qwery.ops.Implicits._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.ops.builtins.Case.When
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
          case ts if ts.nextIf("**") => for (a <- expression; b <- parseNextExpression(ts)) yield a ** b
          case ts if ts.nextIf("/") => for (a <- expression; b <- parseNextExpression(ts)) yield a / b
          case ts if ts.nextIf("||") => for (a <- expression; b <- parseNextExpression(ts)) yield Concat(a, b)
          case _ => None
        }
        // if the expression was resolved ...
        if (result.nonEmpty) expression = result else done = true
      }
    } while (!done && expression.nonEmpty && stream.hasNext)
    expression
  }

  /**
    * Parses an internal or user-defined function (e.g. "LEN('Hello World')")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression internal function]]
    */
  private def parseFunction(stream: TokenStream): Option[Expression] = {
    stream match {
      // is it a parameter-less function? (e.g. "Now()")
      case ts if function0s.exists(fx => ts is fx.name) =>
        function0s.find(f => ts.nextIf(f.name)) map { fx =>
          ts.expect("(").expect(")"); fx
        }
      // is it a single-parameter function? (e.g. "Trim('Hello ')")
      case ts if function1s.exists { case (name, _) => ts is name } =>
        function1s.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
          parseArguments(ts, name, 1) match {
            case a :: Nil => fx(a)
            case params => ts.die(s"Invalid parameters: expected 1, found ${params.size}")
          }
        }
      // is it a two-parameter function? (e.g. "Left('Hello World', 6)")
      case ts if function2s.exists { case (name, _) => ts is name } =>
        function2s.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
          parseArguments(ts, name, 2) match {
            case a :: b :: Nil => fx(a, b)
            case params => ts.die(s"Invalid parameters: expected 2, found ${params.size}")
          }
        }
      // is it a three-parameter function? (e.g. "SubString('Hello World', 6, 5)")
      case ts if function3s.exists { case (name, _) => ts is name } =>
        function3s.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
          parseArguments(ts, name, 3) match {
            case a :: b :: c :: Nil => fx(a, b, c)
            case params => ts.die(s"Invalid parameters: expected 3, found ${params.size}")
          }
        }
      // must be a user-defined function
      case ts => Option(FunctionRef(name = ts.next().text, parseArguments(ts)))
    }
  }

  /**
    * Extracts a variable number of function arguments
    * @param ts the given [[TokenStream token stream]]
    * @return a collection of [[Expression argument expressions]]
    */
  private def parseArguments(ts: TokenStream): List[Expression] = {
    ts.expect("(")
    var args: List[Expression] = Nil
    while (!ts.is(")")) {
      args = parseExpression(ts).getOrElse(ts.die("An expression was expected")) :: args
      if (!ts.is(")")) ts.expect(",")
    }
    ts.expect(")")
    args.reverse
  }

  /**
    * Extracts a fixed number of function arguments
    * @param ts    the given [[TokenStream token stream]]
    * @param name  the name of the function
    * @param count the number of arguments to expect
    * @return a collection of [[Expression argument expressions]]
    */
  private def parseArguments(ts: TokenStream, name: String, count: Int): List[Expression] = {
    val args = parseArguments(ts)
    if (args.size != count) {
      count match {
        case 0 => ts.die(s"Function $name expects no parameters")
        case 1 => ts.die(s"Function $name expects a single parameter")
        case n => ts.die(s"Function $name expects $n parameters")
      }
    }
    args
  }

  private def parseNextCondition(stream: TokenStream): Option[Condition] = {
    stream match {
      case ts if ts nextIf "NOT" => parseNOT(ts)
      case ts =>
        var condition: Option[Condition] = None
        var expression: Option[Expression] = None
        var done: Boolean = false

        do {
          if (expression.isEmpty) expression = parseExpression(ts)
          else if (ts nextIf "IS") {
            if (condition.nonEmpty) ts.die("Illegal start of expression")
            val useNot = ts nextIf "NOT"
            ts expect "NULL"
            condition = expression map IsNull
            if (useNot) condition = condition.map(NOT)
          }
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
      // is it a Case expression?
      case ts if ts nextIf "Case" => parseCase(ts)
      // is it a special function?
      case ts if ts nextIf "Cast" => parseCast(ts)
      // is it an all fields reference?
      case ts if ts nextIf "*" => Option(AllFields)
      // is it a variable?
      case ts if ts is "@" => parseVariableRef(ts)
      // is it a quantity (e.g. "(2 + (5 * 2))")?
      case ts if ts nextIf "(" =>
        val expr = parseExpression(ts)
        ts expect ")"
        expr
      // is it a function?
      case ts if ts.matches(identifierRegEx) & ts.peekAhead(1).exists(_ is "(") => parseFunction(stream)
      // is it a field or constant value?
      case ts if ts.isNumeric | ts.isQuoted => Option(Expression(ts.next()))
      case ts if ts.matches(identifierRegEx) | ts.isBackticks => Option(Field(ts))
      case _ => None
    }
  }

  def parseVariableRef(ts: TokenStream): Option[VariableRef] = {
    if (ts nextIf "@") {
      val name = ts.next().text
      if (isIdentifier(name)) Option(VariableRef(name))
      else ts.die("Variable identifier expected")
    }
    else None
  }

  /**
    * Parses an expression alias (e.g. "(1 + 3) * 2 AS qty")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression CAST expression]]
    */
  def parseNamedAlias(stream: TokenStream, expression: Expression): NamedExpression = {
    stream match {
      case ts if ts.isBackticks | ts.matches(identifierRegEx) => NamedExpression(name = ts.next().text, expression)
      case ts => ts.die("Identifier expected for alias")
    }
  }

  /**
    * Parses a CASE expression
    *
    * Syntax 1:
    * {{{
    * CASE primary-expr
    *   WHEN expr1 THEN result-expr1
    *   WHEN expr2 THEN result-expr2
    *   ELSE expr3
    * END
    * }}}
    *
    * Syntax 2:
    * {{{
    * CASE
    *   WHEN primary-expr = expr1 THEN result-expr1
    *   WHEN primary-expr = expr2 THEN result-expr2
    *   ELSE expr3
    * END
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return
    */
  private def parseCase(ts: TokenStream): Option[Expression] = {
    var cases: List[When] = Nil
    var done = false
    var otherwise: Option[Expression] = None

    // is there a primary expression?
    val primaryExpr = parseExpression(ts)

    while (!done && ts.nextIf("WHEN")) {
      // get the condition
      val condition = {
        if (primaryExpr.nonEmpty) {
          for {
            expr0 <- primaryExpr
            expr1 <- parseExpression(ts)
          } yield EQ(expr0, expr1)
        }
        else parseCondition(ts)
      } getOrElse ts.die("Conditional expression expected")

      // get the result
      ts.expect("THEN")
      val result = parseExpression(ts) getOrElse ts.die("Results expression expected")

      // else case?
      if (ts nextIf "ELSE") {
        otherwise = parseExpression(ts)
        if (otherwise.isEmpty) ts.die("Else expression expected")
        done = true
      }

      // add the case
      cases = cases ::: When(condition, result) :: Nil
    }
    ts.expect("END")
    Option(Case(conditions = cases, otherwise = otherwise))
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
  val identifierRegEx = "[_a-zA-Z][_a-zA-Z0-9]{0,30}"
  private val function0s = Seq(Now, Rand, Uuid)
  private val function1s = Map(
    "AVG" -> Avg.apply _,
    "COUNT" -> Count.apply _,
    "LEN" -> Len.apply _,
    "MAX" -> Max.apply _,
    "MIN" -> Min.apply _,
    "SIGN" -> Sign.apply _,
    "SQRT" -> Sqrt.apply _,
    "SUM" -> Sum.apply _,
    "TRIM" -> Trim.apply _
  )
  private val function2s = Map(
    "CONCAT" -> Concat.apply _,
    "DATE_FORMAT" -> DateFormat.apply _,
    "DATE_PARSE" -> DateParse.apply _,
    "LEFT" -> Left.apply _,
    "PADLEFT" -> PadLeft.apply _,
    "PADRIGHT" -> PadRight.apply _,
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
    "LIKE" -> LIKE.apply _,
    "MATCHES" -> MATCHES.apply _
  )

  /**
    * Indicates whether the given name qualifies as an identifier (e.g. "customerName")
    * @param name the given name
    * @return true, if the given name qualifies as an identifier
    */
  def isIdentifier(name: String): Boolean = name.matches(identifierRegEx)

}