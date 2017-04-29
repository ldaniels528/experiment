package com.github.ldaniels528.qwery

/**
  * Represents an evaluatable value
  * @author lawrence.daniels@gmail.com
  */
trait Evaluatable {

  def compare(that: Evaluatable, data: Map[String, Any]): Int

  def evaluate(data: Map[String, Any]): Option[Any]

}

/**
  * Evaluatable Companion
  * @author lawrence.daniels@gmail.com
  */
object Evaluatable {

  def apply(value: Any): Evaluatable = value match {
    case v: Double => NumericValue(v)
    case v: String => StringValue(v)
    case v =>
      throw new IllegalArgumentException(s"Invalid value type '$v' (${Option(v).map(_.getClass.getName).orNull})")
  }
}
