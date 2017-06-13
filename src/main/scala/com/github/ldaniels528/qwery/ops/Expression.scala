package com.github.ldaniels528.qwery.ops

import java.util.{Date, UUID}

import com.github.ldaniels528.qwery._

import scala.util.Try

/**
  * Represents an expression; while in its simplest form is a value (boolean, double or string)
  * @author lawrence.daniels@gmail.com
  */
trait Expression {
  private val booleans = Seq("on", "true", "yes")

  def compare(that: Expression, scope: Scope): Int = {
    evaluate(scope).map(Expression.apply).map(_.compare(that, scope)) getOrElse -1
  }

  def evaluate(scope: Scope): Option[Any]

  def getAsBoolean(scope: Scope): Option[Boolean] = evaluate(scope).map(_.asInstanceOf[Object]) map {
    case value: Number => value.doubleValue() != 0
    case value: String => booleans.exists(_.equalsIgnoreCase(value))
    case value => value.toString.equalsIgnoreCase("true")
  }

  def getAsByte(scope: Scope): Option[Byte] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.byteValue())
    case value: String => Try(value.toByte).toOption
    case value => Try(value.toString.toByte).toOption
  }

  def getAsDate(scope: Scope): Option[Date] = evaluate(scope) flatMap {
    case value: Double => Option(new Date(value.toLong))
    case value: Long => Option(new Date(value))
    case value: Date => Option(value)
    case _ => None
  }

  def getAsDouble(scope: Scope): Option[Double] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.doubleValue())
    case value: String => Try(value.toDouble).toOption
    case value => Try(value.toString.toDouble).toOption
  }

  def getAsFloat(scope: Scope): Option[Float] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.floatValue())
    case value: String => Try(value.toFloat).toOption
    case value => Try(value.toString.toFloat).toOption
  }

  def getAsInt(scope: Scope): Option[Int] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.intValue())
    case value: String => Try(value.toInt).toOption
    case value => Try(value.toString.toInt).toOption
  }

  def getAsLong(scope: Scope): Option[Long] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.longValue())
    case value: String => Try(value.toLong).toOption
    case value => Try(value.toString.toLong).toOption
  }

  def getAsShort(scope: Scope): Option[Short] = evaluate(scope).map(_.asInstanceOf[Object]) flatMap {
    case value: Number => Option(value.shortValue())
    case value: String => Try(value.toShort).toOption
    case value => Try(value.toString.toShort).toOption
  }

  def getAsString(scope: Scope): Option[String] = evaluate(scope) map {
    case value: String => value
    case value => value.toString
  }

  def getAsUUID(scope: Scope): Option[UUID] = evaluate(scope) flatMap {
    case value: UUID => Some(value)
    case value => Try(UUID.fromString(value.toString)).toOption
  }

}

/**
  * Expression Companion
  * @author lawrence.daniels@gmail.com
  */
object Expression {
  val validTypes = Seq("Boolean", "Byte", "Double", "Float", "Int", "Integer", "Long", "Short", "String", "UUID")

  def isValidType(typeName: String): Boolean = validTypes.exists(_.equalsIgnoreCase(typeName))

  /**
    * Returns a [[Expression]] implementation for the given raw value
    * @param value the given raw value
    * @return a [[Expression]]
    */
  def apply(value: Any): Expression = value.asInstanceOf[Object] match {
    case t: Token => apply(t.value)
    case v => ConstantValue(v)
  }

}
