package com.github.ldaniels528.qwery.ops.types

import java.text.SimpleDateFormat
import java.util.Date

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Represents a date value
  * @author lawrence.daniels@gmail.com
  */
case class DateValue(value: Date) extends Expression {
  private lazy val stringValue = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(value)

  override def compare(that: Expression, scope: Scope): Int = {
    that.evaluate(scope).map {
      case d: Date => value.compareTo(d)
      case _ => -1
    } getOrElse -1
  }

  override def evaluate(scope: Scope): Option[Date] = Option(value)

  override def toString: String = stringValue

}

/**
  * Date Value Companion
  * @author lawrence.daniels@gmail.com
  */
object DateValue {

  def apply(ts: Long): DateValue = DateValue(new Date(ts))

}