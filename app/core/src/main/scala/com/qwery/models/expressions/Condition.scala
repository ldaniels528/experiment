package com.qwery.models.expressions

/**
  * Base class for all conditional expressions
  * @author lawrence.daniels@gmail.com
  */
trait Condition extends Expression

/**
  * Represents a conditional expression
  * @param expr0  the left-side [[Expression]]
  * @param expr1  the right-side [[Expression]]
  * @param codeOp the Spark operator (e.g. "!==")
  * @param sqlOp  the SQL operator (e.g. "<>")
  */
case class ConditionalOp(expr0: Expression, expr1: Expression, codeOp: String, sqlOp: String) extends Condition

/**
  * ConditionalOp Companion
  * @author lawrence.daniels@gmail.com
  */
object ConditionalOp {

  /**
    * Creates a conditional expression
    * @param expr0 the left-side [[Expression]]
    * @param expr1 the right-side [[Expression]]
    * @param op    the Spark operator (e.g. "!==")
    */
  def apply(expr0: Expression, expr1: Expression, op: String) = new ConditionalOp(expr0, expr1, op, op)

}

/**
  * SQL: `condition` AND `condition`
  * @param a the left-side [[Condition condition]]
  * @param b the right-side [[Condition condition]]
  */
case class AND(a: Condition, b: Condition) extends Condition

/**
  * SQL: `expression` BETWEEN `expression` AND `expression`
  * @param expr tthe [[Expression expression]] to evaluate
  * @param from the lower bound [[Expression expression]]
  * @param to   the upper bound [[Expression expression]]
  */
case class Between(expr: Expression, from: Expression, to: Expression) extends Condition

/**
  * SQL: `expression` IS NOT NULL 
  * @param expr the [[Expression expression]] to evaluate
  */
case class IsNotNull(expr: Expression) extends Condition

/**
  * SQL: `expression` IS NULL
  * @param expr the [[Expression expression]] to evaluate
  */
case class IsNull(expr: Expression) extends Condition

/**
  * SQL: `expression` LIKE `pattern`
  * @param a the [[Expression expression]] to evaluate
  * @param b the pattern [[Expression expression]]
  */
case class LIKE(a: Expression, b: Expression) extends Condition

/**
  * SQL: NOT `condition`
  * @param condition the [[Condition condition]] to evaluate
  */
case class NOT(condition: Condition) extends Condition

/**
  * SQL: `condition` OR `condition`
  * @param a the left-side [[Condition condition]]
  * @param b the right-side [[Condition condition]]
  */
case class OR(a: Condition, b: Condition) extends Condition

/**
  * SQL: `expression` RLIKE `pattern`
  * @param a the [[Expression expression]] to evaluate
  * @param b the pattern [[Expression expression]]
  */
case class RLIKE(a: Expression, b: Expression) extends Condition