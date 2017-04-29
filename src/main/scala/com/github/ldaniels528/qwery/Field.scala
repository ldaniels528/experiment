package com.github.ldaniels528.qwery

/**
  * Represents a field
  * @author lawrence.daniels@gmail.com
  */
case class Field(name: String) extends Evaluatable {

  override def compare(that: Evaluatable, data: Map[String, Any]): Int = {
    data.get(name).map(v => Evaluatable.apply(v).compare(that, data)) getOrElse -1
  }

  override def evaluate(data: Map[String, Any]): Option[Any] = data.get(name)
}

/**
  * Field Companion
  * @author lawrence.daniels@gmail.com
  */
object Field {

  def apply(token: Token): Field = Field(token.text)

}

/**
  * Represents a numeric value
  * @author lawrence.daniels@gmail.com
  */
case class NumericValue(value: Double) extends Evaluatable {

  override def compare(that: Evaluatable, data: Map[String, Any]): Int = {
    that match {
      case NumericValue(v) => value.compareTo(v)
      case StringValue(s) => value.toString.compareTo(s)
      case field: Field => field.compare(this, data)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

  override def evaluate(data: Map[String, Any]): Option[Double] = Option(value)
}

/**
  * Represents a string value
  * @author lawrence.daniels@gmail.com
  */
case class StringValue(value: String) extends Evaluatable {

  override def compare(that: Evaluatable, data: Map[String, Any]): Int = {
    that match {
      case NumericValue(v) => value.compareTo(v.toString)
      case StringValue(v) => value.compareTo(v)
      case field: Field => field.compare(this, data)
      case unknown =>
        throw new IllegalStateException(s"Unhandled value '$unknown' (${Option(unknown).map(_.getClass.getName).orNull})")
    }
  }

  override def evaluate(data: Map[String, Any]): Option[String] = Option(value)
}
