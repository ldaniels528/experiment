package com.qwery.database

import com.qwery.database.ColumnTypes.ColumnType
import com.qwery.database.functions.lookupTransformationFunction
import com.qwery.database.types._
import com.qwery.models.Invokable
import com.qwery.models.expressions.Case.When
import com.qwery.models.expressions._
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Expression Virtual Machine
 */
object ExpressionVM {

  def evaluate(condition: Condition)(implicit scope: Scope): QxBoolean = QxBoolean(isTrue(condition))

  def evaluate(expression: Expression)(implicit scope: Scope): QxAny = {
    expression match {
      case BasicField(name) => QxAny(scope.get(name))
      case Case(whens, otherwise) =>
        (whens collectFirst {
          case When(cond, result) if isTrue(cond) => evaluate(result)
        }) ?? otherwise.map(evaluate) || QxNull
      case Cast(expr, toType) => evaluate(expr) // TODO cast to requested type
      case CurrentRow => scope.currentRow
      case fc@FunctionCall(name, args) =>
        //scope.getFunction(name).evaluate(args.map(evaluate))
        val fxTemplate = lookupTransformationFunction(name)
        val fx = fxTemplate(fc.alias || nextID, args)
        QxAny(fx.execute(KeyValues()))
      case If(cond, trueValue, falseValue) => evaluate(if (isTrue(cond)) trueValue else falseValue)
      case Literal(value) => QxAny(value)
      case MathOp(expr0, expr1, operator) => evaluate(expr0, expr1, operator)
      case Null => QxNull
      case unknown => throw new RuntimeException(s"Unhandled expression '$unknown'")
    }
  }

  private def evaluate(expr0: Expression, expr1: Expression, operator: String)(implicit scope: Scope): QxAny = {
    (evaluate(expr0), evaluate(expr1)) match {
      case (a: QxNumber, b: QxNumber) =>
        operator match {
          case "+" => a + b
          case "-" => a - b
          case "%" => a % b
          case "*" => a * b
          case "/" => a / b
          case unknown => throw new RuntimeException(s"Unhandled math operator '$unknown'")
        }
      case (s: QxString, n: QxNumber) =>
        operator match {
          case "*" => s * QxInt(n.toInt)
          case unknown => throw new RuntimeException(s"Unhandled operator '$unknown'")
        }
      case (a, b) =>
        operator match {
          case "+" => a + b
          case unknown => throw new RuntimeException(s"Unhandled operator '$unknown'")
        }
    }
  }

  def isFalse(condition: Condition)(implicit scope: Scope): Boolean = !isTrue(condition)

  def isTrue(condition: Condition)(implicit scope: Scope): Boolean = {
    condition match {
      case AND(a, b) => isTrue(a) && isTrue(b)
      case Between(expr, from, to) =>
        val (value, a, b) = (evaluate(expr), evaluate(from), evaluate(to))
        (value >= a) && (value <= b)
      case ConditionalOp(expr0, expr1, operator, _) => isTrue(expr0, expr1, operator)
      case Exists(query) => compile(query).nonEmpty
      case IN(BasicField(name), source) =>
        val (value, rows) = (scope.get(name), compile(source))
        rows.exists(_.get(name) == value)
      case IsNull(expr) => evaluate(expr).value.isEmpty
      case IsNotNull(expr) => evaluate(expr).value.nonEmpty
      case LIKE(a, b) =>
        (evaluate(a), evaluate(b)) match {
          case (text: QxString, subtext: QxString) => text.contains(subtext)
          case _ => throw new RuntimeException(s"Type mismatch in '$a LIKE $b'")
        }
      case NOT(cond) => isFalse(cond)
      case OR(a, b) => isTrue(a) || isTrue(b)
      case RLIKE(a, b) =>
        (evaluate(a), evaluate(b)) match {
          case (text: QxString, regex: QxString) => text.matches(regex)
          case _ => throw new RuntimeException(s"Type mismatch in '$a RLIKE $b'")
        }
      case unknown => throw new RuntimeException(s"Unhandled condition '$unknown'")
    }
  }

  private def compile(invokable: Invokable): Iterator[KeyValues] = {
    ???
  }

  private def isTrue(expr0: Expression, expr1: Expression, operator: String)(implicit scope: Scope): Boolean = {
    val (a, b) = (evaluate(expr0), evaluate(expr1))
    operator match {
      case "=" | "==" => a === b
      case "!=" | "<>" => a >= b
      case "<" => a < b
      case ">" => a > b
      case "<=" => a <= b
      case ">=" => a >= b
      case unknown => throw new RuntimeException(s"Unhandled conditional operator '$unknown'")
    }
  }

  def nextID: String = java.lang.Long.toString(System.currentTimeMillis(), 36)

  /**
   * Rich Expression
   * @param expression the host [[Expression expression]]
   */
  final implicit class RichExpression(val expression: Expression) extends AnyVal {

    @inline def returnType: ColumnType = ???

  }

}
