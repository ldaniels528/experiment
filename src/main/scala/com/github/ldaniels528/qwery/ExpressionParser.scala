package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.ExpressionParser._
import com.github.ldaniels528.qwery.ops.Field.AllFields
import com.github.ldaniels528.qwery.ops.{BuiltinFunction, Field, Scope, Value}

import scala.util.Try

/**
  * Expression Parser
  * @author lawrence.daniels@gmail.com
  */
trait ExpressionParser {
  private val functions = Map(
    "AVG" -> AvgFx.apply _,
    "COUNT" -> CountFx.apply _,
    "MAX" -> MaxFx.apply _,
    "MIN" -> MinFx.apply _,
    "SQRT" -> SqrtFx.apply _,
    "SUM" -> SumFx.apply _
  )

  def parseExpressions(stream: TokenStream): Option[Value] = {
    var expression: Option[Value] = None
    var done: Boolean = false

    def update(result: Option[Value]): Unit = {
      if (result.nonEmpty) {
        if (expression.isEmpty) expression = result
        else throw new SyntaxException(s"Don't know what to do ($expression)", stream.peek.orNull)
      }
      else done = true
    }

    while (stream.hasNext && !done) {
      if (expression.nonEmpty) {
        // for now done ...
        done = true
      }
      else {
        update(stream match {
          // is it a function?
          case ts if ts.peekAhead(1).exists(_.text == "(") => Option(parseFunctions(stream))
          case ts =>
            ts.nextOption.flatMap {
              case t if t.text == "*" => Option(AllFields)
              case t: AlphaToken => Option(Field(t))
              case t: QuotedToken => Option(if (t.isBackticks) Field(t) else Value(t))
              case t: NumericToken => Option(Value(t))
              case _ =>
                stream.previous
                None
            }
        })
      }
    }
    expression
  }

  /**
    * Parses an internal function (e.g. "SUM(amount)")
    * @param ts the given [[TokenStream token stream]]
    * @return an [[BuiltinFunction internal function]]
    */
  private def parseFunctions(ts: TokenStream) = {
    functions.find { case (name, _) => ts.peek.exists(_.is(name)) } map { case (_, fx) =>
      ts.next()
      ts.expect("(")
      val field = ts.nextOption.map(Field.apply) getOrElse (throw new SyntaxException("Field reference expected"))
      ts.expect(")")
      fx(field)
    } getOrElse (throw new SyntaxException("Function expected", ts.previous.orNull))
  }

}

/**
  * Expression Parser Singleton
  * @author lawrence.daniels@gmail.com
  */
object ExpressionParser {

  /**
    * AVG function
    * @param field the field to count
    */
  case class AvgFx(field: Field) extends BuiltinFunction(isAggregatable = true, isAggregateOnly = true) {
    private var total = 0d
    private var count = 0L

    override def evaluate(scope: Scope): Option[Double] = {
      if (count > 0) Some(total / count) else None
    }

    override def update(scope: Scope): Unit = {
      count += 1
      total += convertToDouble(scope, field) getOrElse 0d
    }
  }

  /**
    * COUNT function
    * @param field the field to count
    */
  case class CountFx(field: Field) extends BuiltinFunction(isAggregatable = true, isAggregateOnly = true) {
    private var count = 0L

    override def evaluate(scope: Scope): Option[Long] = Some(count)

    override def update(scope: Scope): Unit = {
      if (field.name == "*" || scope.get(field.name).nonEmpty) count += 1
    }
  }

  /**
    * MAX function
    * @param field the field to count
    */
  case class MaxFx(field: Field) extends BuiltinFunction(isAggregatable = true, isAggregateOnly = true) {
    private var maximum = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(maximum)

    override def update(scope: Scope): Unit = {
      convertToDouble(scope, field).foreach(v => maximum = Math.max(maximum, v))
    }
  }

  /**
    * MIN function
    * @param field the field to count
    */
  case class MinFx(field: Field) extends BuiltinFunction(isAggregatable = true, isAggregateOnly = true) {
    private var minimum = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(minimum)

    override def update(scope: Scope): Unit = {
      convertToDouble(scope, field).foreach(v => minimum = Math.min(minimum, v))
    }
  }

  /**
    * SQRT function
    * @param field the field to count
    */
  case class SqrtFx(field: Field) extends BuiltinFunction(isAggregatable = false, isAggregateOnly = false) {

    override def evaluate(scope: Scope): Option[Double] = {
      convertToDouble(scope, field).map(Math.sqrt)
    }

    override def update(scope: Scope): Unit = ()
  }

  /**
    * SUM function
    * @param field the field to count
    */
  case class SumFx(field: Field) extends BuiltinFunction(isAggregatable = true, isAggregateOnly = true) {
    private var total = 0d

    override def evaluate(scope: Scope): Option[Double] = Some(total)

    override def update(scope: Scope): Unit = total += convertToDouble(scope, field).getOrElse(0d)
  }

  private def convertToDouble(scope: Scope, field: Field): Option[Double] = scope.get(field.name) flatMap {
    case value: String => Try(value.toDouble).toOption
    case value: Double => Option(value)
    case value: Int => Option(value.toDouble)
    case value: Long => Option(value.toDouble)
    case value => Try(value.toString.toDouble).toOption
  }

}