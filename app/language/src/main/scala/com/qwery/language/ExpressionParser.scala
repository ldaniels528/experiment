package com.qwery.language

import com.qwery.language.ExpressionParser._
import com.qwery.language.TokenStreamHelpers._
import com.qwery.models.expressions._
import com.qwery.models.expressions.implicits._

/**
  * Expression Parser
  * @author lawrence.daniels@gmail.com
  */
trait ExpressionParser {
  private val processor = new ExpressionTemplateProcessor {}

  /**
    * Parses a condition from the given stream
    * @param stream the given [[TokenStream stream]]
    * @return the option of a [[Condition]]
    */
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

  /**
    * Parses an expression from the given stream
    * @param stream the given [[TokenStream stream]]
    * @return the option of a [[Expression]]
    */
  def parseExpression(stream: TokenStream): Option[Expression] = {
    import com.qwery.models.expressions.NativeFunctions._
    var expression: Option[Expression] = None
    var done: Boolean = false
    do {
      if (expression.isEmpty) expression = parseNextExpression(stream)
      else {
        val result = stream match {
          case ts if ts is "[" => expression.flatMap(array => parseArrayIndex(array, ts))
          case ts if ts nextIf "+" => for (a <- expression; b <- parseNextExpression(ts)) yield a + b
          case ts if ts nextIf "-" => for (a <- expression; b <- parseNextExpression(ts)) yield a - b
          case ts if ts nextIf "*" => for (a <- expression; b <- parseNextExpression(ts)) yield a * b
          case ts if ts nextIf "**" => for (a <- expression; b <- parseNextExpression(ts)) yield a ** b
          case ts if ts nextIf "/" => for (a <- expression; b <- parseNextExpression(ts)) yield a / b
          case ts if ts nextIf "%" => for (a <- expression; b <- parseNextExpression(ts)) yield a % b
          case ts if ts nextIf "&" => for (a <- expression; b <- parseNextExpression(ts)) yield a & b
          case ts if ts nextIf "|" => for (a <- expression; b <- parseNextExpression(ts)) yield a | b
          case ts if ts nextIf "^" => for (a <- expression; b <- parseNextExpression(ts)) yield a ^ b
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
    * Parses a SQL BETWEEN clause
    * @param stream the given [[TokenStream token stream]]
    * @return the option of a new [[Condition]]
    */
  private def parseBetween(stream: TokenStream): Option[Condition] = {
    for {
      expr <- parseNextExpression(stream)
      _ = stream expect "BETWEEN"
      a <- parseNextExpression(stream)
      _ = stream expect "AND"
      b <- parseNextExpression(stream)
    } yield BETWEEN(expr, a, b)
  }

  /**
    * Creates a new field from a token stream
    * @param stream the given [[TokenStream token stream]]
    * @return a new [[Field field]] instance
    */
  protected def parseField(stream: TokenStream): Field = {
    import TokenStreamHelpers._
    stream match {
      case ts if ts nextIf "*" => AllFields
      case ts if ts.isJoinColumn => parseJoinField(ts) getOrElse (throw SyntaxException("Invalid field alias", ts))
      case ts if ts.isField => Field(ts.next().text)
      case ts => ts.die(s"Token is not valid (type: ${ts.peek.map(_.getClass.getName).orNull})")
    }
  }

  /**
    * Parses an internal or user-defined function (e.g. "LEN('Hello World')")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression internal function]]
    */
  private def parseFunction(stream: TokenStream): Option[Expression] = {
    import NativeFunction._
    stream match {
      // is it a native function?
      case ts if nativeFunctions.exists { case (name, _) => ts is name } =>
        nativeFunctions.find { case (name, _) => ts nextIf name } map { case (name, fx) =>
          val args = parseArguments(ts)
          if (args.length < fx.minArgs) ts.die(s"At least ${fx.minArgs} arguments expected for function $name")
          else if (args.length > fx.maxArgs) ts.die(s"Too many arguments for function $name")
          else FunctionCall(name, args)
        }

      // must be a user-defined function
      case ts => Option(FunctionCall(name = ts.next().text, parseArguments(ts)))
    }
  }

  /**
    * Parses an IF(condition, trueValue, falseValue) statement
    * @param ts the given [[TokenStream token stream]]
    * @return the option of an [[If]]
    */
  private def parseIf(ts: TokenStream): Option[If] = {
    val results = processor.process("( %c:condition , %e:true , %e:false )", ts)(this)
    for {
      condition <- results.conditions.get("condition")
      trueValue <- results.expressions.get("true")
      falseValue <- results.expressions.get("false")
    } yield If(condition, trueValue, falseValue)
  }

  /**
    * Parses an IfNull(condition, expression) statement
    * @param ts the given [[TokenStream token stream]]
    * @return the option of an [[IfNull]]
    */
  private def parseIfNull(ts: TokenStream): Option[IfNull] = {
    val results = processor.process("( %c:condition , %e:expression )", ts)(this)
    for {
      condition <- results.conditions.get("condition")
      expression <- results.expressions.get("expression")
    } yield IfNull(condition, expression)
  }

  /**
    * Extracts a variable number of function arguments
    * @param ts the given [[TokenStream token stream]]
    * @return a collection of [[Expression argument expressions]]
    */
  private def parseArguments(ts: TokenStream): List[Expression] = {
    processor.process("( %E:args )", ts)(this).expressionLists.getOrElse("args", Nil)
  }

  private def parseArrayIndex(array: Expression, ts: TokenStream): Option[Expression] = {
    import com.qwery.models.expressions.NativeFunctions._
    val results = processor.process("[ %e:index ]", ts)(this)
    results.expressions.get("index") map { index => Array_Position(array, index) }
  }

  private def parseNextCondition(stream: TokenStream): Option[Condition] = {
    stream match {
      case ts if ts.peekAhead(1).exists(_ is "BETWEEN") => parseBetween(ts)
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
      case ts if ts nextIf "case" => parseCase(ts)
      // is it a Cast function?
      case ts if ts nextIf "cast" => parseCast(ts)
      // is it an If expression?
      case ts if ts nextIf "if" => parseIf(ts)
      // is it an IfNull expression?
      case ts if ts nextIf "ifNull" => parseIfNull(ts)
      // is is a null value?
      case ts if ts nextIf "null" => Null
      // is it a constant value?
      case ts if ts.isConstant => Literal(value = ts.next().value)
      // is it an all fields reference?
      case ts if ts nextIf "*" => AllFields
      // is it a variable? (e.g. @totalCost)
      case ts if (ts is "@") | (ts is "$") => parseVariableRef(ts) map {
        case v: LocalVariableRef => v
        case _: RowSetVariableRef => ts.die("Row set variables cannot be used in expressions")
        case _ => ts.die("Unsupported expression; a column variable was expected (e.g. $myVar)")
      }
      // is it a quantity (e.g. "(2 + (x * 2))")?
      case ts if ts is "(" => parseQuantity(ts)
      // is it a function?
      case ts if ts.isFunction => parseFunction(ts)
      // is it a join (aliased) column reference (e.g. "A.Symbol")?
      case ts if ts.isJoinColumn => parseJoinField(ts)
      // is it a field?
      case ts if ts.isField => parseField(ts)
      // unmatched
      case _ => None
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
    var cases: List[Case.When] = Nil
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
      cases = Case.When(condition, result) :: cases

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
    val results = processor.process("( %e:value AS %t:type )", ts)(this)
    for {
      value <- results.expressions.get("value")
      toType <- results.types.get("type")
    } yield Cast(value, toType)
  }

  /**
    * Parses a field with an alias (e.g. "A.Symbol")
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[Field]]
    */
  private def parseJoinField(ts: TokenStream): Option[JoinField] = {
    val results = processor.process("%a:alias . %a:name", ts)(this)
    for {name <- results.atoms.get("name"); alias <- results.atoms.get("alias")} yield JoinField(name, tableAlias = Option(alias))
  }

  /**
    * Parses a NOT condition (e.g. "NOT X = 1")
    * @param ts the given [[TokenStream token stream]]
    * @return a [[Condition condition]]
    */
  private def parseNOT(ts: TokenStream): Option[NOT] =
    Option(NOT(parseCondition(ts).getOrElse(ts.die("Conditional expression expected"))))

  /**
    * Parses a variable reference
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[VariableRef]]
    */
  private def parseVariableRef(ts: TokenStream): Option[VariableRef] =
    processor.process("%v:variable", ts)(this).variables.get("variable")

  /**
    * Parses an expression quantity (e.g. "(x * 2)")
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[Expression]]
    */
  private def parseQuantity(ts: TokenStream): Option[Expression] =
    processor.process("( %e:expr )", ts)(this).expressions.get("expr")

}

/**
  * Expression Parser Singleton
  * @author lawrence.daniels@gmail.com
  */
object ExpressionParser {
  private val conditionalOps: Map[String, (Expression, Expression) => Condition] = Map(
    "=" -> EQ,
    ">=" -> GE,
    ">" -> GT,
    "<=" -> LE,
    "<" -> LT,
    "<>" -> NE,
    "!=" -> NE,
    "LIKE" -> LIKE,
    "RLIKE" -> RLIKE
  )

}
