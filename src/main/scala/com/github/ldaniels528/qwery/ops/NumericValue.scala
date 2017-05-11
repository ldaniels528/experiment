package com.github.ldaniels528.qwery.ops

/**
  * Represents a numeric value
  * @author lawrence.daniels@gmail.com
  */
case class NumericValue(value: Double) extends Expression {

  override def compare(that: Expression, scope: Scope): Int = {
    that match {
      case NumericValue(v) => value.compareTo(v)
      case StringValue(s) => value.toString.compareTo(s)
      case field: Field => field.compare(this, scope)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

  override def evaluate(scope: Scope): Option[Double] = Option(value)

  override def toString: String = value.toString

}
