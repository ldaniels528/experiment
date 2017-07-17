package com.github.ldaniels528.qwery.util

/**
  * Tuple Helper
  * @author lawrence.daniels@gmail.com
  */
object TupleHelper {

  implicit class TupleConversion[T](val values: Seq[T]) extends AnyVal {

    def toTuple2: (T, T) = values.toList match {
      case List(a, b, _*) => a -> b
      case _ => throw new IllegalArgumentException("Could not convert to tuple2")
    }

    def toTuple3: (T, T, T) = values.toList match {
      case List(a, b, c, _*) => (a, b, c)
      case _ => throw new IllegalArgumentException("Could not convert to tuple3")
    }

    def toTuple4: (T, T, T, T) = values.toList match {
      case List(a, b, c, d, _*) => (a, b, c, d)
      case _ => throw new IllegalArgumentException("Could not convert to tuple4")
    }

    def toTuple5: (T, T, T, T, T) = values.toList match {
      case List(a, b, c, d, e, _*) => (a, b, c, d, e)
      case _ => throw new IllegalArgumentException("Could not convert to tuple5")
    }

    def toTuple6: (T, T, T, T, T, T) = values.toList match {
      case List(a, b, c, d, e, f, _*) => (a, b, c, d, e, f)
      case _ => throw new IllegalArgumentException("Could not convert to tuple6")
    }

    def toTuple7: (T, T, T, T, T, T, T) = values.toList match {
      case List(a, b, c, d, e, f, g, _*) => (a, b, c, d, e, f, g)
      case _ => throw new IllegalArgumentException("Could not convert to tuple7")
    }

    def toTuple8: (T, T, T, T, T, T, T, T) = values.toList match {
      case List(a, b, c, d, e, f, g, h, _*) => (a, b, c, d, e, f, g, h)
      case _ => throw new IllegalArgumentException("Could not convert to tuple8")
    }

    def toTuple9: (T, T, T, T, T, T, T, T, T) = values.toList match {
      case List(a, b, c, d, e, f, g, h, i, _*) => (a, b, c, d, e, f, g, h, i)
      case _ => throw new IllegalArgumentException("Could not convert to tuple9")
    }

    def toTuple10: (T, T, T, T, T, T, T, T, T, T) = values.toList match {
      case List(a, b, c, d, e, f, g, h, i, j, _) => (a, b, c, d, e, f, g, h, i, j)
      case _ => throw new IllegalArgumentException("Could not convert to tuple10")
    }

    def toTuple11: (T, T, T, T, T, T, T, T, T, T, T) = values.toList match {
      case List(a, b, c, d, e, f, g, h, i, j, k, _) => (a, b, c, d, e, f, g, h, i, j, k)
      case _ => throw new IllegalArgumentException("Could not convert to tuple11")
    }

  }

}
