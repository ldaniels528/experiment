package com.qwery.language

import com.qwery.language.ExpressionParser._
import com.qwery.language.TokenStreamHelpers._
import com.qwery.models.expressions.implicits._
import com.qwery.models.expressions.{NativeFunctions => f, _}

/**
  * Expression Parser
  * @author lawrence.daniels@gmail.com
  */
trait ExpressionParser {
  private val processor = new ExpressionTemplateProcessor {}
  private val slp = new SQLLanguageParser {}

  /**
    * Parses a conditional expression from the given stream
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
          case ts if ts nextIf "AND" => for (a <- condition; b <- parseNextCondition(ts)) yield a && b
          case ts if ts nextIf "OR" => for (a <- condition; b <- parseNextCondition(ts)) yield a || b
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
          case ts if ts nextIf "||" => for (a <- expression; b <- parseNextExpression(ts)) yield f.concat(a, b)
          case _ => None
        }
        // if the expression was resolved ...
        if (result.nonEmpty) expression = result else done = true
      }
    } while (!done && expression.nonEmpty && stream.hasNext)
    expression
  }

  /**
    * Extracts a variable number of function arguments
    * @param ts the given [[TokenStream token stream]]
    * @return a collection of [[Expression argument expressions]]
    */
  protected def parseArguments(ts: TokenStream): List[Expression] = {
    processor.process("( %E:args )", ts)(this).expressionLists.getOrElse("args", Nil)
  }

  /**
    * Extracts an array index from the stream
    * @param array the array [[Expression expression]]
    * @param ts    the given [[TokenStream token stream]]
    * @return the option of a new [[Expression expression]]
    */
  protected def parseArrayIndex(array: Expression, ts: TokenStream): Option[Expression] = {
    val results = processor.process("[ %e:index ]", ts)(this)
    results.expressions.get("index") map { index => f.array_position(array, index) }
  }

  /**
    * Parses a SQL BETWEEN clause
    * @param expression the given [[Expression]]
    * @param stream     the given [[TokenStream token stream]]
    * @return the option of a new [[Condition]]
    */
  protected def parseBetween(expression: Option[Expression], stream: TokenStream): Option[Condition] = {
    for {
      expr <- expression
      a <- parseExpression(stream)
      _ = stream expect "AND"
      b <- parseExpression(stream)
    } yield expr.between(a, b)
  }

  /**
    * Parses a SQL EXISTS clause
    * @param stream the given [[TokenStream token stream]]
    * @return the option of a new [[Condition]]
    */
  protected def parseExists(stream: TokenStream): Option[Condition] = {
    Option(Exists(slp.parseNextQueryOrVariable(stream)))
  }

  /**
    * Creates a new field from a token stream
    * @param stream the given [[TokenStream token stream]]
    * @return a new [[FieldRef field]] instance
    */
  protected def parseField(stream: TokenStream): FieldRef = {
    import TokenStreamHelpers._
    stream match {
      case ts if ts nextIf "*" => AllFields
      case ts if ts.isJoinColumn => parseJoinField(ts) getOrElse ts.die("Invalid field alias")
      case ts if ts.isField => FieldRef(ts.next().text)
      case ts if ts.isNumeric => FieldRef(ts.next().text)
      case ts => ts.die(s"Token is not valid (type: ${ts.peek.map(_.getClass.getName).orNull})")
    }
  }

  /**
    * Parses an internal or user-defined function (e.g. "LEN('Hello World')")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression internal function]]
    */
  protected def parseFunction(stream: TokenStream): Option[Expression] = {
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
    * @return the option of an [[If if]] expression
    */
  protected def parseIf(ts: TokenStream): Option[If] = {
    val results = processor.process("( %c:condition , %e:true , %e:false )", ts)(this)
    for {
      condition <- results.conditions.get("condition")
      trueValue <- results.expressions.get("true")
      falseValue <- results.expressions.get("false")
    } yield If(condition, trueValue, falseValue)
  }

  /**
    * Parses a SQL IN clause
    * @param expression the given [[Expression expression]]
    * @param stream     the given [[TokenStream token stream]]
    * @return the option of a new [[Condition condition]]
    */
  protected def parseIn(expression: Option[Expression], stream: TokenStream): Option[Condition] = {
    expression map { expr =>
      stream match {
        case ts if ts.isSubQuery => IN(expr, slp.parseIndirectQuery(stream)(slp.parseNextQueryOrVariable))
        case ts =>
          processor.processOpt("( %E:args )", ts)(this) match {
            case Some(template) => IN(expr)(template.expressionLists.getOrElse("args", ts.die("Unexpected empty list")): _*)
            case None => IN(expr, slp.parseNextQueryOrVariable(stream))
          }
      }
    }
  }

  /**
    * Extracts an INTERVAL expression (e.g. "INTERVAL 7 DAYS")
    * @param stream the given [[TokenStream token stream]]
    * @return the option of a new [[Expression expression]]
    */
  protected def parseInterval(stream: TokenStream): Option[Expression] = {
    import IntervalTypes._
    for {
      count <- parseExpression(stream)
      (_, intervalType) = intervalTypes.find { case (name, _) => (stream nextIf name) || (stream nextIf name + "S") }
        .getOrElse(stream.die("Invalid interval type"))
    } yield Interval(count, intervalType)
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
  protected def parseCase(ts: TokenStream): Option[Expression] = {
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
          for {expr0 <- primaryExpr; expr1 <- parseExpression(ts)} yield expr0 === expr1
        }
      } getOrElse ts.die("Conditional expression expected")

      // parse the 'THEN' expression
      val result = parseExpression(ts.expect("THEN")) getOrElse ts.die("Results expression expected")

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
    * @return the option of an [[Expression CAST expression]]
    */
  protected def parseCast(ts: TokenStream): Option[Cast] = {
    val results = processor.process("( %e:value AS %t:type )", ts)(this)
    for {
      value <- results.expressions.get("value")
      toType <- results.types.get("type")
    } yield Cast(value, toType)
  }


  /**
    * Parses a DISTINCT function/expression (e.g. "DISTINCT firstName, lastName")
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[Distinct DISTINCT function/expression]]
    */
  protected def parseDistinct(ts: TokenStream): Option[Distinct] = {
    val template = if (ts is "(") "( %E:args )" else "%E:args"
    val expressions = processor.process(template, ts)(this).expressionLists.getOrElse("args", Nil)
    Option(Distinct(expressions = expressions))
  }

  /**
    * Parses a field with an alias (e.g. "A.Symbol")
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[FieldRef field]]
    */
  protected def parseJoinField(ts: TokenStream): Option[JoinFieldRef] = {
    val results = processor.process("%a:alias . %a:name", ts)(this)
    for {name <- results.atoms.get("name"); alias <- results.atoms.get("alias")} yield JoinFieldRef(name, tableAlias = Option(alias))
  }

  /**
    * Parses the next conditional expression from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return the option of a new [[Condition conditional expression]]
    */
  protected def parseNextCondition(stream: TokenStream): Option[Condition] = {
    stream match {
      case ts if ts nextIf "EXISTS" => parseExists(ts)
      case ts if ts nextIf "NOT" => parseNOT(ts)
      case ts =>
        var condition: Option[Condition] = None
        var expression: Option[Expression] = None
        var done: Boolean = false
        var isNot: Boolean = false

        do {
          if (expression.isEmpty) expression = parseExpression(ts)
          else {
            // special handling for NOT
            while (ts nextIf "NOT") isNot = !isNot

            // check all other conditions
            if (ts nextIf "BETWEEN") condition = parseBetween(expression, ts)
            else if (ts nextIf "IN") condition = parseIn(expression, ts)
            else if (ts nextIf "IS") {
              if (condition.nonEmpty) ts.die("Illegal start of expression")
              val isNot = ts nextIf "NOT"
              ts expect "NULL"
              condition = if (isNot) expression map IsNotNull else expression map IsNull
            }
            // handle conditional operators
            else {
              val result = for {
                a <- expression
                (_, op) <- conditionalOps.find { case (symbol, _) => ts nextIf symbol }
                b <- parseExpression(ts)
              } yield op(a, b)
              if (result.nonEmpty) condition = result else done = true
            }

            // was a negative result indicated?
            if(isNot) {
              condition = condition.map(NOT.apply)
              isNot = false
            }
          }
        } while (!done && expression.nonEmpty && ts.hasNext)
        condition
    }
  }

  /**
    * Parses the next expression from the stream
    * @param stream the given [[TokenStream token stream]]
    * @return the option of a new [[Expression expression]]
    */
  protected def parseNextExpression(stream: TokenStream): Option[Expression] = {
    import com.qwery.util.OptionHelper.Implicits.Risky._
    stream match {
      case ts if ts nextIf "CASE" => parseCase(ts)
      case ts if ts nextIf "CAST" => parseCast(ts)
      case ts if ts nextIf "CURRENT ROW" => Option(CurrentRow)
      case ts if ts nextIf "DISTINCT" => parseDistinct(ts)
      case ts if ts nextIf "IF" => parseIf(ts)
      case ts if ts nextIf "INTERVAL" => parseInterval(ts)
      case ts if ts nextIf "NULL" => Null
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
    * Parses a NOT condition (e.g. "NOT X = 1")
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[Condition condition]]
    */
  protected def parseNOT(ts: TokenStream): Option[NOT] =
    Option(NOT(parseCondition(ts).getOrElse(ts.die("Conditional expression expected"))))

  /**
    * Parses a variable reference
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[VariableRef]]
    */
  protected def parseVariableRef(ts: TokenStream): Option[VariableRef] =
    processor.process("%v:variable", ts)(this).variables.get("variable")

  /**
    * Parses an expression quantity (e.g. "(x * 2)")
    * @param ts the given [[TokenStream token stream]]
    * @return the option of a [[Expression]]
    */
  protected def parseQuantity(ts: TokenStream): Option[Expression] =
    processor.process("( %e:expr )", ts)(this).expressions.get("expr")

}

/**
  * Expression Parser Singleton
  * @author lawrence.daniels@gmail.com
  */
object ExpressionParser {
  protected val conditionalOps: Map[String, (Expression, Expression) => Condition] = Map(
    "=" -> { (expr0, expr1) => expr0 === expr1 },
    ">=" -> { (expr0, expr1) => expr0 >= expr1 },
    ">" -> { (expr0, expr1) => expr0 > expr1 },
    "<=" -> { (expr0, expr1) => expr0 <= expr1 },
    "<" -> { (expr0, expr1) => expr0 < expr1 },
    "<>" -> { (expr0, expr1) => expr0 !== expr1 },
    "!=" -> { (expr0, expr1) => expr0 !== expr1 },
    "LIKE" -> LIKE,
    "RLIKE" -> RLIKE
  )

}
