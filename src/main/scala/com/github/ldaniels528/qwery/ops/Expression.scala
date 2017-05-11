package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery._

import scala.util.Try

/**
  * Represents an expression; while in its simplest form is a value (boolean, double or string)
  * @author lawrence.daniels@gmail.com
  */
trait Expression {

  def compare(that: Expression, scope: Scope): Int = {
    evaluate(scope).map(Expression.apply).map(_.compare(that, scope)) getOrElse -1
  }

  def evaluate(scope: Scope): Option[Any]

  def getAsBoolean(scope: Scope): Option[Boolean] = evaluate(scope).map(_.asInstanceOf[Object]) map {
    case value: Number => value.doubleValue() != 0
    case value: String => value.equalsIgnoreCase("true")
    case value => value.toString.equalsIgnoreCase("true")
  }

  def getAsDouble(scope: Scope): Option[Double] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.doubleValue())
    case value: String => Try(value.toDouble).toOption
    case value => Try(value.toString.toDouble).toOption
  }

  def getAsLong(scope: Scope): Option[Long] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.longValue())
    case value: String => Try(value.toLong).toOption
    case value => Try(value.toString.toLong).toOption
  }

  def getAsString(scope: Scope): Option[String] = evaluate(scope) map {
    case value: String => value
    case value => value.toString
  }

}

/**
  * Expression Companion
  * @author lawrence.daniels@gmail.com
  */
object Expression {

  /**
    * Returns a [[Expression]] implementation for the given raw value
    * @param value the given raw value
    * @return a [[Expression]]
    */
  def apply(value: Any): Expression = value.asInstanceOf[Object] match {
    case v: Number => NumericValue(v.doubleValue())
    case v: String => StringValue(v)
    case t: Token => apply(t.value)
    case v =>
      throw new IllegalArgumentException(s"Invalid value type '$v' (${Option(v).map(_.getClass.getName).orNull})")
  }

}
