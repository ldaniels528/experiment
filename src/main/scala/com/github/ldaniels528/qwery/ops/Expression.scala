package com.github.ldaniels528.qwery.ops

/**
  * Represents a conditional expression
  * @author lawrence.daniels@gmail.com
  */
sealed trait Expression {
  def satisfies(scope: Scope): Boolean
}

case class AND(a: Expression, b: Expression) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.satisfies(scope) && b.satisfies(scope)
}

case class EQ(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.compare(b, scope) == 0
}

case class GT(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.compare(b, scope) > 0
}

case class GE(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.compare(b, scope) >= 0
}

case class LIKE(a: Evaluatable, b: String) extends Expression {
  override def satisfies(scope: Scope): Boolean = a match {
    case StringValue(s) => s.matches(b)
    case _ => false
  }
}

case class LT(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.compare(b, scope) < 0
}

case class LE(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.compare(b, scope) >= 0
}

case class NE(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.compare(b, scope) != 0
}

case class NOT(expr: Expression) extends Expression {
  override def satisfies(scope: Scope): Boolean = !expr.satisfies(scope)
}

case class OR(a: Expression, b: Expression) extends Expression {
  override def satisfies(scope: Scope): Boolean = a.satisfies(scope) || b.satisfies(scope)
}
