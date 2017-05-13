package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.ops.types.StringValue

/**
  * Represents a conditional expression
  * @author lawrence.daniels@gmail.com
  */
sealed trait Condition extends SQLLike {
  def isSatisfied(scope: Scope): Boolean
}

case class AND(a: Condition, b: Condition) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.isSatisfied(scope) && b.isSatisfied(scope)

  override def toSQL: String = s"$a AND $b"
}

case class EQ(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) == 0

  override def toSQL: String = s"$a = $b"
}

case class GT(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) > 0

  override def toSQL: String = s"$a > $b"
}

case class GE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) >= 0

  override def toSQL: String = s"$a >= $b"
}

case class LIKE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a match {
    case StringValue(s) => b.getAsString(scope).exists(s.matches)
    case _ => false
  }

  override def toSQL: String = s"$a LIKE $b"
}

case class LT(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) < 0

  override def toSQL: String = s"$a < $b"
}

case class LE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) >= 0

  override def toSQL: String = s"$a <= $b"
}

case class NE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) != 0

  override def toSQL: String = s"$a <> $b"
}

case class NOT(expr: Condition) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = !expr.isSatisfied(scope)

  override def toSQL: String = s"NOT $expr"
}

case class OR(a: Condition, b: Condition) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.isSatisfied(scope) || b.isSatisfied(scope)

  override def toSQL: String = s"$a OR $b"
}
