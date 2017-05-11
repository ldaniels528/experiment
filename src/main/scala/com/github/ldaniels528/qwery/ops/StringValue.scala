package com.github.ldaniels528.qwery.ops

/**
  * Represents a string value
  * @author lawrence.daniels@gmail.com
  */
case class StringValue(value: String) extends Expression {

  override def compare(that: Expression, scope: Scope): Int = {
    that match {
      case NumericValue(v) => value.compareTo(v.toString)
      case StringValue(v) => value.compareTo(v)
      case field: Field => field.compare(this, scope)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

  override def evaluate(scope: Scope): Option[String] = Option(value)

  override def toString: String = value

}