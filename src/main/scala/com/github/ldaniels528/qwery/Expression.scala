package com.github.ldaniels528.qwery

/**
  * Represents a conditional expression
  * @author lawrence.daniels@gmail.com
  */
sealed trait Expression {
  def satisfies(data: Map[String, Any]): Boolean
}

case class AND(a: Expression, b: Expression) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.satisfies(data) && b.satisfies(data)
}

case class EQ(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.compare(b, data) == 0
}

case class GT(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.compare(b, data) > 0
}

case class GE(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.compare(b, data) >= 0
}

case class LIKE(a: Evaluatable, b: String) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a match {
    case StringValue(s) => s.matches(b)
    case _ => false
  }
}

case class LT(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.compare(b, data) < 0
}

case class LE(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.compare(b, data) >= 0
}

case class NE(a: Evaluatable, b: Evaluatable) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.compare(b, data) != 0
}

case class NOT(expr: Expression) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = !expr.satisfies(data)
}

case class OR(a: Expression, b: Expression) extends Expression {
  override def satisfies(data: Map[String, Any]): Boolean = a.satisfies(data) || b.satisfies(data)
}
