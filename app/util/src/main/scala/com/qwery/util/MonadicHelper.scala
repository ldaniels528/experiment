package com.qwery.util

import scala.util.Try

/**
  * Monadic Helper
  * @author lawrence.daniels@gmail.com
  */
object MonadicHelper {

  /**
    * Option[T] Enrichment
    * @param a the given [[Option]]
    */
  final implicit class OptionEnrichment[T](val a: Option[T]) extends AnyVal {

    @inline def ===(b: T): Boolean = a.contains(b)

    @inline def !==(b: T): Boolean = !a.contains(b)

  }

  /**
    * Option[Boolean] Enrichment
    * @param option the given [[Option]]
    */
  final implicit class OptionBooleanEnrichment[A <: Boolean](val option: Option[A]) extends AnyVal {

    @inline def isTrue: Boolean = option.contains(true)

    @inline def isFalse: Boolean = option.contains(false)

  }

  /**
    * {{{ Option[T <: Ordered[T]] }}} Enrichment
    * @param a the given [[Option]] of an int value
    */
  final implicit class OptionOrderedEnrichment[T <: Ordered[T]](val a: Option[T]) extends AnyVal {

    @inline def <(b: T): Boolean = a.exists(_ < b)

    @inline def <(b: Option[T]): Boolean = (for (aa <- a; bb <- b) yield aa < bb).isTrue

    @inline def <=(b: T): Boolean = a.exists(_ <= b)

    @inline def <=(b: Option[T]): Boolean = (for (aa <- a; bb <- b) yield aa <= bb).isTrue

    @inline def >(b: T): Boolean = a.exists(_ > b)

    @inline def >(b: Option[T]): Boolean = (for (aa <- a; bb <- b) yield aa > bb).isTrue

    @inline def >=(b: T): Boolean = a.exists(_ >= b)

    @inline def >=(b: Option[T]): Boolean = (for (aa <- a; bb <- b) yield aa >= bb).isTrue

  }

  /**
    * Try[T] Enrichment
    * @param a the given [[Try]] of a value of type T
    */
  final implicit class TryEnrichment[T](val a: Try[T]) extends AnyVal {

    @inline def ===(b: T): Boolean = a.contains(b)

    @inline def !==(b: T): Boolean = !a.contains(b)

    @inline def contains(b: T): Boolean = a.toOption.contains(b)

    @inline def exists(f: T => Boolean): Boolean = a.toOption.exists(f)

  }

  /**
    * {{{ Try[T <: Ordered[T]] }}} Enrichment
    * @param a the given [[Try]] of an ordered value
    */
  final implicit class TryOrderedEnrichment[T <: Ordered[T]](val a: Try[T]) extends AnyVal {

    @inline def <(b: T): Boolean = a.exists(_ < b)

    @inline def <(b: Try[T]): Boolean = (for (aa <- a; bb <- b) yield aa < bb).toOption.isTrue

    @inline def <=(b: T): Boolean = a.exists(_ <= b)

    @inline def <=(b: Try[T]): Boolean = (for (aa <- a; bb <- b) yield aa <= bb).toOption.isTrue

    @inline def >(b: T): Boolean = a.exists(_ > b)

    @inline def >(b: Try[T]): Boolean = (for (aa <- a; bb <- b) yield aa > bb).toOption.isTrue

    @inline def >=(b: T): Boolean = a.exists(_ >= b)

    @inline def >=(b: Try[T]): Boolean = (for (aa <- a; bb <- b) yield aa >= bb).toOption.isTrue

  }

}
