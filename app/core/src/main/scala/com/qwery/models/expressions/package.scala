package com.qwery.models

/**
  * expressions package object
  * @author lawrence.daniels@gmail.com
  */
package object expressions {

  /**
    * Typed Expression compiler
    * @param expression the given [[Expression]]
    */
  final implicit class TypedExpressionCompiler(val expression: Expression) extends AnyVal {
    @inline def asAny: Any = expression match {
      case Literal(value) => value
      case unknown =>
        throw new IllegalArgumentException(s"Unsupported expression '$unknown' primitive value expected.")
    }

    @inline def asInt: Int = expression.asNumber.toInt

    @inline def asNumber: Double = expression match {
      case Literal(value: Double) => value
      case Literal(value: Long) => value
      case Literal(value: Int) => value
      case unknown =>
        throw new IllegalArgumentException(s"Unsupported expression '$unknown' numeric value expected.")
    }

    @inline def asString: String = expression match {
      case Literal(value: String) => value
      case unknown =>
        throw new IllegalArgumentException(s"Unsupported expression '$unknown' string expected.")
    }
  }

}
