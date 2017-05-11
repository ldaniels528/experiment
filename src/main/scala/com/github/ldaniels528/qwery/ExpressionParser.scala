package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.util.OptionHelper._
import com.github.ldaniels528.qwery.ExpressionParser._
import com.github.ldaniels528.qwery.ops.Field.AllFields
import com.github.ldaniels528.qwery.ops._

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

  private def parseNextCondition(stream: TokenStream): Option[Condition] = {
    var condition: Option[Condition] = None
    var expression: Option[Expression] = None
    var done: Boolean = false
    do {
      expression match {
        case Some(exp0) =>
          val result = for {
            (_, op) <- ConditionalOps.find { case (symbol, _) => stream.nextIf(symbol) }
            exp1 <- parseExpression(stream)
          } yield op(exp0, exp1)
          if (result.nonEmpty) condition = result else done = true
        case None =>
          expression = parseExpression(stream)
      }
    } while (!done && expression.nonEmpty && stream.hasNext)
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
          case _ => None
        }
        // if the expression was resolved ...
        if (result.nonEmpty) expression = result else done = true
      }
    } while (!done && expression.nonEmpty && stream.hasNext)
    expression
  }

  private def parseNextExpression(stream: TokenStream): Option[Expression] = {
    stream match {
      // is it an all fields reference?
      case ts if ts.nextIf("*") => Option(AllFields)
      // is it a quantity (e.g. "(2 + (5 * 2))")
      case ts if ts.nextIf("(") =>
        val expr = parseExpression(ts)
        ts.expect(")")
        expr
      // is it a function?
      case ts if /*ts.matches(IdentifierRegEx) &*/ ts.peekAhead(1).exists(_.is("(")) =>
        parseFieldFunction(stream) ?? parseValueFunction(stream)
      // is it a simple value
      case ts =>
        ts.nextOption.flatMap {
          case t: AlphaToken => Option(Field(t))
          case t: QuotedToken => Option(if (t.isBackticks) Field(t) else Expression(t))
          case t: NumericToken => Option(Expression(t))
          case _ =>
            stream.previous
            None
        }
    }
  }

  /**
    * Parses an internal function (e.g. "SUM(amount)")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[InternalFunction internal function]]
    */
  private def parseFieldFunction(ts: TokenStream): Option[InternalFunction] = {
    FieldFunctions.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
      ts.expect("(")
      val result = parseExpression(ts) match {
        case Some(field: Field) => fx(field)
        case _ =>
          throw new SyntaxException(s"Function $name expects a field reference")
      }
      ts.expect(")")
      result
    }
  }

  /**
    * Parses an internal function (e.g. "LEN(name)")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[InternalFunction internal function]]
    */
  private def parseValueFunction(ts: TokenStream): Option[InternalFunction] = {
    ValueFunctions.find { case (name, _) => ts.nextIf(name) } map { case (name, fx) =>
      ts.expect("(")
      val expression = parseExpression(ts)
        .getOrElse(throw new SyntaxException(s"Function $name expects an expression"))
      ts.expect(")")
      println(s"parseValueFunction: expression => $expression (${Option(expression).map(_.getClass.getSimpleName).orNull})")
      fx(expression)
    }
  }

}

/**
  * Expression Parser Singleton
  * @author lawrence.daniels@gmail.com
  */
object ExpressionParser {
  private val IdentifierRegEx = "[_a-zA-Z][_a-zA-Z0-9]{0,30}"
  private val ValueFunctions = Map(
    "LEN" -> LenFx.apply _,
    "SQRT" -> SqrtFx.apply _
  )
  private val FieldFunctions = Map(
    "AVG" -> AvgFx.apply _,
    "COUNT" -> CountFx.apply _,
    "MAX" -> MaxFx.apply _,
    "MIN" -> MinFx.apply _,
    "SUM" -> SumFx.apply _
  )
  private val ConditionalOps = Map(
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

    override def toString: String = s"$a + $b"
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
    * @param field the field to count
    */
  case class AvgFx(field: Field) extends InternalFunction(isAggregatable = true, isAggregateOnly = true) {
    private var total = 0d
    private var count = 0L

    override def evaluate(scope: Scope): Option[Double] = if (count > 0) Some(total / count) else None

    override def update(scope: Scope): Unit = {
      count += 1
      total += field.getAsDouble(scope) getOrElse 0d
    }

    override def toString: String = s"AVG($field)"
  }

  /**
    * COUNT function
    * @param field the field to count
    */
  case class CountFx(field: Field) extends InternalFunction(isAggregatable = true, isAggregateOnly = true) {
    private var count = 0L

    override def evaluate(scope: Scope): Option[Long] = Some(count)

    override def update(scope: Scope): Unit = {
      if (field.name == "*" || scope.get(field.name).nonEmpty) count += 1
    }

    override def toString: String = s"COUNT($field)"
  }

  /**
    * LEN function
    * @param expression the field to count
    */
  case class LenFx(expression: Expression) extends InternalFunction(isAggregatable = false, isAggregateOnly = false) {
    override def evaluate(scope: Scope): Option[Int] = getAsString(scope).map(_.length)

    override def update(scope: Scope): Unit = ()

    override def toString: String = s"LEN($expression)"
  }

  /**
    * MAX function
    * @param field the field to count
    */
  case class MaxFx(field: Field) extends InternalFunction(isAggregatable = true, isAggregateOnly = true) {
    private var maximum = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(maximum)

    override def update(scope: Scope): Unit = {
      field.getAsDouble(scope).foreach(v => maximum = Math.max(maximum, v))
    }

    override def toString: String = s"MAX($field)"
  }

  /**
    * MIN function
    * @param field the field to count
    */
  case class MinFx(field: Field) extends InternalFunction(isAggregatable = true, isAggregateOnly = true) {
    private var minimum = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(minimum)

    override def update(scope: Scope): Unit = {
      field.getAsDouble(scope).foreach(v => minimum = Math.min(minimum, v))
    }

    override def toString: String = s"MIN($field)"
  }

  /**
    * SQRT function
    * @param expression the field to count
    */
  case class SqrtFx(expression: Expression) extends InternalFunction(isAggregatable = false, isAggregateOnly = false) {
    override def evaluate(scope: Scope): Option[Double] = expression.getAsDouble(scope).map(Math.sqrt)

    override def update(scope: Scope): Unit = ()

    override def toString: String = s"SQRT($expression)"
  }

  /**
    * SUM function
    * @param field the field to count
    */
  case class SumFx(field: Field) extends InternalFunction(isAggregatable = true, isAggregateOnly = true) {
    private var total = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(total)

    override def update(scope: Scope): Unit = total += field.getAsDouble(scope).getOrElse(0d)

    override def toString: String = s"SUM($field)"
  }

  /**
    * Expression Extensions
    * @param value the given [[Expression value]]
    */
  implicit class ExpressionExtensions(val value: Expression) extends AnyVal {

    def +(that: Expression) = Add(value, that)

    def -(that: Expression) = Subtract(value, that)

    def *(that: Expression) = Multiply(value, that)

    def /(that: Expression) = Divide(value, that)

  }

}