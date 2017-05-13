package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ExpressionParser._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.qwery.util.OptionHelper._

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
          case ts if ts.nextIf("AND") =>
            for {a <- condition; b <- parseNextCondition(ts)} yield AND(a, b)
          case ts if ts.nextIf("OR") =>
            for {a <- condition; b <- parseNextCondition(ts)} yield OR(a, b)
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
          case ts if ts.nextIf("+") => for {a <- expression; b <- parseNextExpression(ts)} yield a + b
          case ts if ts.nextIf("-") => for {a <- expression; b <- parseNextExpression(ts)} yield a - b
          case ts if ts.nextIf("*") => for {a <- expression; b <- parseNextExpression(ts)} yield a * b
          case ts if ts.nextIf("/") => for {a <- expression; b <- parseNextExpression(ts)} yield a / b
          case ts if ts.nextIf("|") => for {a <- expression; b <- parseNextExpression(ts)} yield a | b
          case _ => None
        }
        // if the expression was resolved ...
        if (result.nonEmpty) expression = result else done = true
      }
    } while (!done && expression.nonEmpty && stream.hasNext)
    expression
  }

  /**
    * Parses an internal expression-based function (e.g. "LEN('Hello World')")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[InternalFunction internal function]]
    */
  private def parseExpressionFunction(ts: TokenStream): Option[InternalFunction] = {
    expressionFunctions.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
      ts.expect("(")
      val expression = parseExpression(ts)
        .getOrElse(ts.die(s"Function $name expects an expression"))
      ts.expect(")")
      fx(expression)
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
      // is it a CAST instruction?
      case ts if ts.nextIf("CAST") => parseCAST(ts)
      // is it an all fields reference?
      case ts if ts.nextIf("*") => Option(AllFields)
      // is it a quantity (e.g. "(2 + (5 * 2))")?
      case ts if ts.nextIf("(") =>
        val expr = parseExpression(ts)
        ts.expect(")")
        expr
      // is it a function?
      case ts if ts.matches(identifierRegEx) & ts.peekAhead(1).exists(_.is("(")) =>
        parseFieldFunction(stream) ?? parseExpressionFunction(stream)
      // is it a field or constant value?
      case ts if ts.isNumeric | ts.isQuoted => Option(Expression(ts.next()))
      case ts if ts.matches(identifierRegEx) | ts.isBackticks => Option(Field(ts.next()))
      case _ => None
    }
  }

  /**
    * Parses an internal field-based function (e.g. "COUNT(amount)")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[InternalFunction internal function]]
    */
  private def parseFieldFunction(ts: TokenStream): Option[InternalFunction] = {
    fieldFunctions.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
      ts.expect("(")
      val result = parseExpression(ts) match {
        case Some(field: Field) => fx(field)
        case _ =>
          ts.die(s"Function $name expects a field reference")
      }
      ts.expect(")")
      result
    }
  }

  /**
    * Parses an expression alias (e.g. "(1 + 3) * 2 AS qty")
    * @param stream the given [[TokenStream token stream]]
    * @return an [[Expression CAST expression]]
    */
  def parseAlias(stream: TokenStream, expression: Expression): Expression = {
    stream match {
      case ts if ts.isBackticks => FieldAlias(ts.next().text, expression)
      case ts if ts.matches(identifierRegEx) => FieldAlias(ts.next().text, expression)
      case ts => ts.die("Identifier expected for alias")
    }
  }

  /**
    * Parses a CAST expression (e.g. "CAST(1234 as String)")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[Expression CAST expression]]
    */
  private def parseCAST(ts: TokenStream): Option[Expression] = {
    ts.expect("(")
    val expression = parseExpression(ts) map { value =>
      ts.expect("AS")
      val toType = ts.nextOption.map(_.text).getOrElse(ts.die("Type expected"))
      ts.expect(")")
      CastFx(value, toType)
    }
    expression getOrElse ts.die("Syntax error")
    expression
  }

  /**
    * Parses a NOT condition (e.g. "NOT X = 1")
    * @param ts the given [[TokenStream token stream]]
    * @return a [[Condition condition]]
    */
  private def parseNOT(ts: TokenStream): Option[Condition] = {
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
  private val fieldFunctions = Map(
    "COUNT" -> CountFx.apply _
  )
  private val expressionFunctions = Map(
    "AVG" -> AvgFx.apply _,
    "LEN" -> LenFx.apply _,
    "MAX" -> MaxFx.apply _,
    "MIN" -> MinFx.apply _,
    "SQRT" -> SqrtFx.apply _,
    "SUM" -> SumFx.apply _
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

  //////////////////////////////////////////////////////////////////////
  //    Math Operations
  //////////////////////////////////////////////////////////////////////

  /**
    * Represents an addition operation
    * @param a the left quantity
    * @param b the right quantity
    */
  case class Add(a: Expression, b: Expression) extends Expression {
    override def evaluate(scope: Scope): Option[Double] = for {
      x <- a.getAsDouble(scope)
      y <- b.getAsDouble(scope)
    } yield x + y

    override def toString: String = s"$a + $b"
  }

  /**
    * Represents an concatenation operation
    * @param a the left quantity
    * @param b the right quantity
    */
  case class Concat(a: Expression, b: Expression) extends Expression {
    override def evaluate(scope: Scope): Option[String] = for {
      x <- a.getAsString(scope)
      y <- b.getAsString(scope)
    } yield x + y

    override def toString: String = s"$a | $b"
  }

  /**
    * Represents a division operation
    * @param a the left quantity
    * @param b the right quantity
    */
  case class Divide(a: Expression, b: Expression) extends Expression {
    override def evaluate(scope: Scope): Option[Double] = for {
      x <- a.getAsDouble(scope)
      y <- b.getAsDouble(scope) if y != 0
    } yield x / y

    override def toString: String = s"$a / $b"
  }

  /**
    * Represents a multiplication operation
    * @param a the left quantity
    * @param b the right quantity
    */
  case class Multiply(a: Expression, b: Expression) extends Expression {
    override def evaluate(scope: Scope): Option[Double] = for {
      x <- a.getAsDouble(scope)
      y <- b.getAsDouble(scope)
    } yield x * y

    override def toString: String = s"$a * $b"
  }

  /**
    * Represents a subtraction operation
    * @param a the left quantity
    * @param b the right quantity
    */
  case class Subtract(a: Expression, b: Expression) extends Expression {
    override def evaluate(scope: Scope): Option[Double] = for {
      x <- a.getAsDouble(scope)
      y <- b.getAsDouble(scope)
    } yield x - y

    override def toString: String = s"$a - $b"
  }

  //////////////////////////////////////////////////////////////////////
  //    Internal Functions
  //////////////////////////////////////////////////////////////////////

  /**
    * AVG function
    * @param expression the expression for which to compute the average
    */
  case class AvgFx(expression: Expression) extends AggregateFunction {
    private var total = 0d
    private var count = 0L

    override def evaluate(scope: Scope): Option[Double] = if (count > 0) Some(total / count) else None

    override def update(scope: Scope): Unit = {
      count += 1
      total += expression.getAsDouble(scope) getOrElse 0d
    }

    override def toString: String = s"AVG($expression)"
  }

  /**
    * CAST function
    * @param value  the expression for which is the input data
    * @param toType the express which describes desired type
    */
  case class CastFx(value: Expression, toType: String) extends InternalFunction {

    override def evaluate(scope: Scope): Option[Any] = {
      toType match {
        case s if s.equalsIgnoreCase("Boolean") => value.getAsBoolean(scope)
        case s if s.equalsIgnoreCase("Double") => value.getAsDouble(scope)
        case s if s.equalsIgnoreCase("Long") => value.getAsLong(scope)
        case s if s.equalsIgnoreCase("String") => value.getAsString(scope)
        case theType =>
          throw new IllegalStateException(s"Invalid conversion type $theType")
      }
    }

    override def toString: String = s"CAST($value AS $toType)"
  }

  /**
    * COUNT function
    * @param field the field to count
    */
  case class CountFx(field: Field) extends AggregateFunction {
    private var count = 0L

    override def evaluate(scope: Scope): Option[Long] = {
      val returnValue = Some(count)
      count = 0L
      returnValue
    }

    override def update(scope: Scope): Unit = {
      if (field.name == "*" || scope.get(field.name).nonEmpty) count += 1
    }

    override def toString: String = s"COUNT($field)"
  }

  /**
    * LEN function
    * @param expression the expression for which to compute the length
    */
  case class LenFx(expression: Expression) extends InternalFunction {
    override def evaluate(scope: Scope): Option[Int] = expression.getAsString(scope).map(_.length)

    override def toString: String = s"LEN($expression)"
  }

  /**
    * MAX function
    * @param expression the expression for which to determine the maximum value
    */
  case class MaxFx(expression: Expression) extends AggregateFunction {
    private var maximum = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(maximum)

    override def update(scope: Scope): Unit = {
      expression.getAsDouble(scope).foreach(v => maximum = Math.max(maximum, v))
    }

    override def toString: String = s"MAX($expression)"
  }

  /**
    * MIN function
    * @param expression the expression for which to determine the minimum value
    */
  case class MinFx(expression: Expression) extends AggregateFunction {
    private var minimum = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(minimum)

    override def update(scope: Scope): Unit = {
      expression.getAsDouble(scope).foreach(v => minimum = Math.min(minimum, v))
    }

    override def toString: String = s"MIN($expression)"
  }

  /**
    * SQRT function
    * @param expression the expression for which to compute the square root
    */
  case class SqrtFx(expression: Expression) extends InternalFunction {
    override def evaluate(scope: Scope): Option[Double] = expression.getAsDouble(scope).map(Math.sqrt)

    override def toString: String = s"SQRT($expression)"
  }

  /**
    * SUM function
    * @param expression the expression to count
    */
  case class SumFx(expression: Expression) extends AggregateFunction {
    private var total = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(total)

    override def update(scope: Scope): Unit = total += expression.getAsDouble(scope).getOrElse(0d)

    override def toString: String = s"SUM($expression)"
  }

  /**
    * Expression Extensions
    * @param value the given [[Expression value]]
    */
  final implicit class ExpressionExtensions(val value: Expression) extends AnyVal {

    def +(that: Expression) = Add(value, that)

    def -(that: Expression) = Subtract(value, that)

    def *(that: Expression) = Multiply(value, that)

    def /(that: Expression) = Divide(value, that)

    def |(that: Expression) = Concat(value, that)

  }

}