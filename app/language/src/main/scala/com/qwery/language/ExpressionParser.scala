package com.qwery.language

import com.qwery.language.ExpressionParser._
import com.qwery.language.TokenStreamHelpers._
import com.qwery.models.expressions.SQLFunction._
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
          case ts if ts nextIf "||" => for (a <- expression; b <- parseNextExpression(ts)) yield Concat(List(a, b))
          case _ => None
        }
        // if the expression was resolved ...
        if (result.nonEmpty) expression = result else done = true
      }
    } while (!done && expression.nonEmpty && stream.hasNext)
    expression
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
      case ts if ts.isBackticks => Field(ts.next().text)
      case ts if ts.isText => Field(ts.next().text)
      case ts => ts.die(s"Token is not valid (type: ${ts.peek.map(_.getClass.getName).orNull})")
    }
  }

  /**
    * Parses a FromUnixTime(timestamp,[format]) function
    * @param ts the given [[TokenStream token stream]]
    * @return a [[From_UnixTime]]
    */
  private def parseFromUnixTime(ts: TokenStream): Option[From_UnixTime] = {
    val results = processor.process("( %E:args )", ts)(this)
    results.expressionLists.get("args") map {
      case timestamp :: Nil => From_UnixTime(timestamp)
      case timestamp :: format :: Nil => From_UnixTime(timestamp, format = Option(format))
      case _ => ts.die("Syntax: FromUnixTime(timestamp,[format])")
    }
  }

  /**
    * Parses an internal or user-defined function (e.g. "LEN('Hello World')")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression internal function]]
    */
  private def parseFunction(stream: TokenStream): Option[Expression] = {
    stream match {
      // is it a no parameter function? (e.g. "Current_Date")
      case ts if function0s.exists { case (name, _) => ts is name } =>
        function0s.find { case (name, _) => ts nextIf name } map { case (_, fx) => fx }
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
      // is it a N-parameter function? (e.g. "Coalesce(dept, 'N/A')")
      case ts if functionNs.exists { case (name, _) => ts is name } =>
        functionNs.find { case (name, _) => ts nextIf name } map { case (_, fx) => fx(parseArguments(ts)) }
      // must be a user-defined function
      case ts => Option(FunctionCall(name = ts.next().text, parseArguments(ts)))
    }
  }

  /**
    * Parses an IF(condition, trueValue, falseValue) expression
    * @param ts the given [[TokenStream token stream]]
    * @return the option of an [[If]]
    */
  private def parseIf(ts: TokenStream): Option[Expression] = {
    val results = processor.process("( %c:condition , %e:true , %e:false )", ts)(this)
    for {
      condition <- results.conditions.get("condition")
      trueValue <- results.expressions.get("true")
      falseValue <- results.expressions.get("false")
    } yield If(condition, trueValue, falseValue)
  }

  /**
    * Extracts a variable number of function arguments
    * @param ts the given [[TokenStream token stream]]
    * @return a collection of [[Expression argument expressions]]
    */
  private def parseArguments(ts: TokenStream): List[Expression] = {
    processor.process("( %E:args )", ts)(this).expressionLists.getOrElse("args", Nil)
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

  private def parseArrayIndex(array: Expression, ts: TokenStream): Option[Array_Index] = {
    val results = processor.process("[ %e:index ]", ts)(this)
    results.expressions.get("index") map { index => Array_Index(array, index) }
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
      case ts if ts nextIf "case" => parseCase(ts)
      // is it a Cast function?
      case ts if ts nextIf "cast" => parseCast(ts)
      // is it a From_UnixTime function?
      case ts if ts nextIf "from_unixtime" => parseFromUnixTime(ts)
      // is it an If expression?
      case ts if ts nextIf "if" => parseIf(ts)
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
  private val function0s = Map(
    "Cume_Dist" -> Cume_Dist,
    "Current_Date" -> Current_Date
  )
  private val function1s = Map(
    "Abs" -> Abs.apply _,
    "Avg" -> Avg.apply _,
    "Ascii" -> Ascii.apply _,
    "Base64" -> Base64.apply _,
    "Bin" -> Bin.apply _,
    "Cbrt" -> Cbrt.apply _,
    "Ceil" -> Ceil.apply _,
    "Count" -> Count.apply _,
    "Distinct" -> Distinct.apply _,
    "Factorial" -> Factorial.apply _,
    "Floor" -> Floor.apply _,
    "Length" -> Length.apply _,
    "Lower" -> Lower.apply _,
    "LTrim" -> LTrim.apply _,
    "Max" -> Max.apply _,
    "Mean" -> Mean.apply _,
    "Min" -> Min.apply _,
    "RTrim" -> RTrim.apply _,
    "StdDev" -> StdDev.apply _,
    "Sum" -> Sum.apply _,
    "To_Date" -> To_Date.apply _,
    "Trim" -> Trim.apply _,
    "Upper" -> Upper.apply _,
    "Variance" -> Variance.apply _,
    "WeekOfYear" -> WeekOfYear.apply _,
    "Year" -> Year.apply _
  )
  private val function2s = Map(
    "Add_Months" -> Add_Months.apply _,
    "Array_Contains" -> Array_Contains.apply _,
    "Date_Add" -> Date_Add.apply _,
    "Split" -> Split.apply _
  )
  private val function3s = Map(
    "LPad" -> LPad.apply _,
    "RPad" -> RPad.apply _,
    "Substr" -> Substring.apply _,
    "Substring" -> Substring.apply _
  )
  private val functionNs = Map(
    "Coalesce" -> Coalesce.apply _,
    "Concat" -> Concat.apply _
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

}
