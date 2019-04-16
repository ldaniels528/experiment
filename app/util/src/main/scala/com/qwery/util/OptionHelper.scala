package com.qwery.util

/**
  * Option Helper
  * @author lawrence.daniels@gmail.com
  */
object OptionHelper {

  object Implicits {

    object Risky {

      final implicit def value2Option[T](value: T): Option[T] = Option(value)

    }

  }

  /**
    * Array Extensions
    * @param array the given array
    */
  final implicit class ArrayExtensions[T](val array: Array[T]) extends AnyVal {

    @inline
    def get(index: Int): Option[T] = if (index < array.length) Option(array(index)) else None

    @inline
    def indexWhereOpt(f: T => Boolean): Option[Int] = array.indexWhere(f) match {
      case -1 => None
      case index => Some(index)
    }
  }

  /**
    * Map Extensions
    * @param m the given [[Map map]]
    */
  final implicit class MapExtensions(val m: Map[String, Any]) extends AnyVal {

    @inline
    def getAs[T](name: String): Option[T] = m.get(name).map(_.asInstanceOf[T])

  }

  /**
    * Boolean Option Enrichment
    * @param option the given [[Option option]]
    */
  final implicit class BooleanOptionEnrichment[A <: Boolean](val option: Option[A]) extends AnyVal {

    @inline
    def isTrue: Boolean = option.contains(true)

    @inline
    def isFalse: Boolean = option.contains(false)

  }

  /**
    * Option Enrichment
    * @param optionA the given [[Option option]]
    */
  final implicit class OptionEnrichment[A](val optionA: Option[A]) extends AnyVal {

    @inline
    def ??(optionB: Option[A]): Option[A] = if (optionA.nonEmpty) optionA else optionB

    @inline
    def ??(value: A): Option[A] = if (optionA.nonEmpty) optionA else Option(value)

    @inline
    def ||(value: A): A = optionA getOrElse value

    @inline
    def orFail(message: => String): A = optionA.getOrElse(throw new IllegalStateException(message))

  }

  final implicit class SequenceEnrichment[T](val values: Seq[T]) extends AnyVal {
    @inline def toOption: Option[Seq[T]] = if(values.isEmpty) None else Some(values)
  }

}
