package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery._

/**
  * Represents a wrapped value
  * @author lawrence.daniels@gmail.com
  */
trait Value {

  def compare(that: Value, scope: Scope): Int

  def evaluate(scope: Scope): Option[Any]

}

/**
  * Value Companion
  * @author lawrence.daniels@gmail.com
  */
object Value {

  def apply(value: Any): Value = value match {
    case v: Double => NumericValue(v)
    case v: String => StringValue(v)
    case t: Token => apply(t.value)
    case v =>
      throw new IllegalArgumentException(s"Invalid value type '$v' (${Option(v).map(_.getClass.getName).orNull})")
  }

  /**
    * Value Sequence Extensions
    * @param values the given collection of values
    */
  implicit class ValueSeqExtensions(val values: Seq[Value]) extends AnyVal {

    @inline
    def isAllFields: Boolean = values.exists {
      case field: Field => field.name == "*"
      case _ => false
    }

  }

}

/**
  * Represents a named value (e.g. field)
  * @author lawrence.daniels@gmail.com
  */
trait NamedValue extends Value {

  def name: String

}

/**
  * Represents a numeric value
  * @author lawrence.daniels@gmail.com
  */
case class NumericValue(value: Double) extends Value {

  override def compare(that: Value, scope: Scope): Int = {
    that match {
      case NumericValue(v) => value.compareTo(v)
      case StringValue(s) => value.toString.compareTo(s)
      case field: Field => field.compare(this, scope)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

  override def evaluate(scope: Scope): Option[Double] = Option(value)
}

/**
  * Represents a string value
  * @author lawrence.daniels@gmail.com
  */
case class StringValue(value: String) extends Value {

  override def compare(that: Value, scope: Scope): Int = {
    that match {
      case NumericValue(v) => value.compareTo(v.toString)
      case StringValue(v) => value.compareTo(v)
      case field: Field => field.compare(this, scope)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

  override def evaluate(scope: Scope): Option[String] = Option(value)
}

