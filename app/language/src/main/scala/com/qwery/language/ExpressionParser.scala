package com.qwery.language

import com.qwery.language.ExpressionParser._
import com.qwery.models.expressions.Expression.Implicits._
import com.qwery.models.expressions._

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
          case ts if ts nextIf "AND" => for (a <- condition; b <- parseNextCondition(ts)) yield AND(a, b)
          case ts if ts nextIf "OR" => for (a <- condition; b <- parseNextCondition(ts)) yield OR(a, b)
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
          case ts if ts nextIf "+" => for (a <- expression; b <- parseNextExpression(ts)) yield a + b
          case ts if ts nextIf "-" => for (a <- expression; b <- parseNextExpression(ts)) yield a - b
          case ts if ts nextIf "*" => for (a <- expression; b <- parseNextExpression(ts)) yield a * b
          case ts if ts nextIf "**" => for (a <- expression; b <- parseNextExpression(ts)) yield a ** b
          case ts if ts nextIf "/" => for (a <- expression; b <- parseNextExpression(ts)) yield a / b
          case ts if ts nextIf "||" => for (a <- expression; b <- parseNextExpression(ts)) yield Concat(a, b)
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
      // is it a single-parameter function? (e.g. "Trim('Hello ')")
      case ts if function1s.exists { case (name, _) => ts is name } =>
        function1s.find { case (name, _) => ts nextIf name } map { case (name, fx) =>
          parseArguments(ts, name, count = 1) match {
            case a :: Nil => fx(a)
            case params => ts.die(s"Invalid parameters: expected 1, found ${params.size}")
          }
        }
      // is it a two-parameter function? (e.g. "Left('Hello World', 6)")
      case ts if function2s.exists { case (name, _) => ts is name } =>
        function2s.find { case (name, _) => ts nextIf name } map { case (name, fx) =>
          parseArguments(ts, name, count = 2) match {
            case a :: b :: Nil => fx(a, b)
            case params => ts.die(s"Invalid parameters: expected 2, found ${params.size}")
          }
        }
      // is it a three-parameter function? (e.g. "SubString('Hello World', 6, 5)")
      case ts if function3s.exists { case (name, _) => ts is name } =>
        function3s.find { case (name, _) => ts nextIf name } map { case (name, fx) =>
          parseArguments(ts, name, count = 3) match {
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
    while (ts isnt ")") {
      args = parseExpression(ts).getOrElse(ts.die("An expression was expected")) :: args
      if (ts isnt ")") ts.expect(",")
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
            val isNot = ts nextIf "NOT"
            ts expect "NULL"
            condition = if (isNot) expression map IsNotNull else expression map IsNull
          }
          else {
            val result = for {
              a <- expression
              (_, op) <- conditionalOps.find { case (symbol, _) => ts nextIf symbol }
              b <- parseExpression(ts)
            } yield op(a, b)
            if (result.nonEmpty) condition = result else done = true
          }
        } while (!done && expression.nonEmpty && ts.hasNext)
        condition
    }
  }

  private def parseNextExpression(stream: TokenStream): Option[Expression] = {
    import com.qwery.util.OptionHelper.Implicits.Risky._
    stream match {
      // is it a Case expression?
      case ts if ts nextIf "CASE" => parseCase(ts)
      // is it a Cast function?
      case ts if ts nextIf "CAST" => parseCast(ts)
      // is it a constant value?
      case ts if ts.isConstant => Literal(value = ts.next().value)
      // is it an all fields reference?
      case ts if ts nextIf "*" => AllFields
      // is it a variable? (e.g. @totalCost)
      case ts if (ts is "@") | (ts is "$") => parseVariableRef(ts)
      // is it a quantity (e.g. "(2 + (x * 2))")?
      case ts if ts is "(" => parseQuantity(ts)
      // is is a null value?
      case ts if ts nextIf "NULL" => Null
      // is it a function?
      case ts if ts.isFunction => parseFunction(ts)
      // is it a join column reference (e.g. "A.Symbol")?
      case ts if ts.isJoinColumn => parseJoinField(ts)
      // is it a field?
      case ts if ts.isField => toField(ts.next())
      case _ => None
    }
  }

  def parseQuantity(ts: TokenStream): Option[Expression] = {
    if (ts nextIf "(") {
      val expr = parseExpression(ts)
      ts expect ")"
      expr
    }
    else None
  }

  def parseJoinField(ts: TokenStream): Option[Field] = {
    val params = SQLTemplateParams(ts, "%a:alias . %a:column")
    Option(BasicField(name = params.atoms("column")).as(params.atoms.get("alias")))
  }

  def parseVariableRef(stream: TokenStream): Option[VariableRef] = {
    stream match {
      case ts if ts nextIf "@" => Option(RowSetVariableRef(ts.next().text))
      case ts if ts nextIf "$" => Option(LocalVariableRef(ts.next().text))
      case ts => ts.die("Variable expected")
    }
  }

  /**
    * Parses an expression alias (e.g. "(1 + 3) * 2 AS qty")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression CAST expression]]
    */
  def parseNamedAlias(stream: TokenStream, expression: Expression): NamedExpression = {
    stream match {
      case ts if ts.isBackticks | ts.isIdentifier => Field(name = ts.next().text, expression)
      case ts => ts.die("Identifier expected for alias")
    }
  }

  /**
    * Parses a CASE expression
    * @example
    * {{{
    * CASE primary-expr
    *   WHEN expr1 THEN result-expr1
    *   WHEN expr2 THEN result-expr2
    *   ELSE expr3
    * END
    * }}}
    * @example
    * {{{
    * CASE
    *   WHEN primary-expr = expr1 THEN result-expr1
    *   WHEN primary-expr = expr2 THEN result-expr2
    *   ELSE expr3
    * END
    * }}}
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a Case expression
    */
  private def parseCase(ts: TokenStream): Option[Expression] = {
    var cases: List[When] = Nil
    var done = false
    var otherwise: Option[Expression] = None

    // is there a primary expression? (e.g. "[CASE] Symbol ...")
    val primaryExpr = if (ts is "WHEN") None else parseExpression(ts)

    // collect the 'WHEN' clauses
    while (!done && (ts nextIf "WHEN")) {
      // parse the condition
      val condition = {
        if (primaryExpr.isEmpty) parseCondition(ts) else {
          for {expr0 <- primaryExpr; expr1 <- parseExpression(ts)} yield EQ(expr0, expr1)
        }
      } getOrElse ts.die("Conditional expression expected")

      // parse the 'THEN' expression
      ts.expect("THEN")
      val result = parseExpression(ts) getOrElse ts.die("Results expression expected")

      // parse the 'ELSE' expression?
      if (ts nextIf "ELSE") {
        if (otherwise.nonEmpty) ts.die("Duplicate case detected")
        otherwise = parseExpression(ts)
        if (otherwise.isEmpty) ts.die("Else expression expected")
        done = true
      }

      // add the case
      cases = When(condition, result) :: cases

      // check for the end of the case
      done = done || (ts is "END")
    }
    ts.expect("END")
    Option(Case(conditions = cases.reverse, otherwise = otherwise))
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
      Cast(value, Literal(value = toType))
    }
    if (expression.isEmpty) ts.die("Syntax error")
    expression
  }

  /**
    * Parses a NOT condition (e.g. "NOT X = 1")
    * @param ts the given [[TokenStream token stream]]
    * @return a [[Condition condition]]
    */
  private def parseNOT(ts: TokenStream): Option[NOT] =
    Option(NOT(parseCondition(ts).getOrElse(ts.die("Conditional expression expected"))))

  private def toField(token: Token): Field = token match {
    case AlphaToken(name, _) => Field(name)
    case t@QuotedToken(name, _, _) if t.isBackTicks => Field(name)
    case t =>
      throw new IllegalArgumentException(s"Token '$t' is not valid (type: ${t.getClass.getName})")
  }

}

/**
  * Expression Parser Singleton
  * @author lawrence.daniels@gmail.com
  */
object ExpressionParser {
  private val identifierRegEx = "[_a-zA-Z][_a-zA-Z0-9]{0,30}"
  private val function1s = Map(
    "AVG" -> Avg.apply _,
    "COUNT" -> Count.apply _,
    "MAX" -> Max.apply _,
    "MIN" -> Min.apply _,
    "STDDEV" -> StdDev.apply _,
    "SUM" -> Sum.apply _
  )
  private val function2s = Map(
    "CAST" -> Cast.apply _,
    "CONCAT" -> Concat.apply _,
    "PADLEFT" -> PadLeft.apply _,
    "PADRIGHT" -> PadRight.apply _
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
    "RLIKE" -> RLIKE.apply _
  )

  /**
    * Token Expression Helpers
    * @param token the given [[Token]]
    */
  final implicit class TokenExpressionHelpers(val token: Token) extends AnyVal {
    @inline def isIdentifier: Boolean = token.matches(identifierRegEx)
  }

  /**
    * TokenStream Expression Helpers
    * @param ts the given [[TokenStream]]
    */
  final implicit class TokenStreamExpressionHelpers(val ts: TokenStream) extends AnyVal {
    @inline def isConstant: Boolean = ts.isNumeric || ts.isQuoted

    @inline def isField: Boolean = ts.isBackticks || (ts.isIdentifier && !ts.isFunction)

    @inline def isFunction: Boolean =
      (for (a <- ts(0); b <- ts(1); _ <- ts(2)) yield a.isIdentifier && (b is "(")).contains(true)

    @inline def isIdentifier: Boolean = ts.peek.exists(_.isIdentifier)

    @inline def isJoinColumn: Boolean =
      (for (a <- ts(0); b <- ts(1); c <- ts(2)) yield a.isIdentifier && (b is ".") && c.isIdentifier).contains(true)

  }

  /**
    * Indicates whether the given name qualifies as an identifier (e.g. "customerName")
    * @param name the given identifier name
    * @return true, if the given name qualifies as an identifier
    */
  def isIdentifier(name: String): Boolean = name.matches(identifierRegEx)

}
