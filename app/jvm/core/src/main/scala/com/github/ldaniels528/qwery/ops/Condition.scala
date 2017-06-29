package com.github.ldaniels528.qwery.ops

/**
  * Represents a conditional expression
  * @author lawrence.daniels@gmail.com
  */
trait Condition {
  def isSatisfied(scope: Scope): Boolean
}

case class AND(a: Condition, b: Condition) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.isSatisfied(scope) && b.isSatisfied(scope)
}

case class EQ(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) == 0
}

case class GT(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) > 0
}

case class GE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) >= 0
}

case class IsNull(a: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.evaluate(scope).isEmpty
}

case class LIKE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = {
    (for {
      string <- a.getAsString(scope)
      pattern <- b.getAsString(scope).map(_.replaceAllLiterally("%", ".*"))
    } yield string.matches(pattern)).contains(true)
  }
}

case class LT(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) < 0
}

case class LE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) >= 0
}

case class MATCHES(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = {
    (for {
      string <- a.getAsString(scope)
      pattern <- b.getAsString(scope)
    } yield string.matches(pattern)).contains(true)
  }
}

case class NE(a: Expression, b: Expression) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.compare(b, scope) != 0
}

case class NOT(expr: Condition) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = !expr.isSatisfied(scope)
}

case class OR(a: Condition, b: Condition) extends Condition {
  override def isSatisfied(scope: Scope): Boolean = a.isSatisfied(scope) || b.isSatisfied(scope)
}
