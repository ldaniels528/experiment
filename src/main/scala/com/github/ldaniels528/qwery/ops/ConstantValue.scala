package com.github.ldaniels528.qwery.ops

import java.lang.{Boolean => JBoolean}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, UUID}

import com.github.ldaniels528.qwery.ops.ConstantValue._

/**
  * Represents a Constant Value
  * @author lawrence.daniels@gmail.com
  */
case class ConstantValue(value: Any) extends Expression {
  private lazy val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  override def compare(that: Expression, scope: Scope): Int = {
    that.evaluate(scope).map(_.asInstanceOf[Object]) map (value.asInstanceOf[Object] -> _) map {
      case (a: Date, b: Date) => a.compareTo(b)
      case (a: JBoolean, b: JBoolean) => a.compareTo(b)
      case (a: Number, b: Number) => a.doubleValue().compareTo(b.doubleValue())
      case (a: String, b: String) => a.compareTo(b)
      case (a: UUID, b: UUID) => a.compareTo(b)
      case _ => -1
    } getOrElse -1
  }

  override def evaluate(scope: Scope): Option[Any] = Option(value)

  override def toString: String = value.asInstanceOf[AnyRef] match {
    case d: Date => dateFormat.format(value)
    case n: Number => numberFormat.format(n)
    case s: String => s
    case x => x.toString
  }
}

/**
  * Constant Value Companion
  * @author lawrence.daniels@gmail.com
  */
object ConstantValue {
  private lazy val numberFormat = new DecimalFormat("###.#####")

}