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

  /**
    * String Enrichment
    * @param string the given [[String string]]
    */
  final implicit class StringEnrichment(val string: String) extends AnyVal {

    @inline
    def indexOfOpt(c: Char): Option[Int] = string.indexOf(c) match {
      case -1 => None
      case index => Some(index)
    }

    @inline
    def indexOfOpt(c: Char, fromIndex: Int): Option[Int] = string.indexOf(c, fromIndex) match {
      case -1 => None
      case index => Some(index)
    }

    @inline
    def indexOfOpt(s: String): Option[Int] = string.indexOf(s) match {
      case -1 => None
      case index => Some(index)
    }

    @inline
    def indexOfOpt(s: String, fromIndex: Int): Option[Int] = string.indexOf(s, fromIndex) match {
      case -1 => None
      case index => Some(index)
    }

    @inline
    def noneIfBlank: Option[String] = {
      val trimmedString = string.trim
      if (trimmedString.isEmpty) None else Some(trimmedString)
    }

    @inline
    def substringOpt(start: Int): Option[String] = {
      if (start >= 0 && start < string.length) Some(string.substring(start)) else None
    }

    @inline
    def substringOpt(start: Int, end: Int): Option[String] = {
      if (start >= 0 && start <= end && end < string.length) Some(string.substring(start, end)) else None
    }
  }

}
