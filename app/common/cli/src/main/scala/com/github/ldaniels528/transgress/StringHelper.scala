package com.github.ldaniels528.transgress

/**
  * String Helper
  * @author lawrence.daniels@gmail.com
  */
object StringHelper {

  /**
    * String Extensions
    * @author lawrence.daniels@gmail.com
    */
  implicit class StringExtensions(val string: String) extends AnyVal {

    @inline
    def extractAll(tok0: String, tok1: String, fromIndex: Int = 0): List[String] = string.findIndices(tok0, tok1, fromIndex) match {
      case Some((p0, p1)) => string.substring(p0 + tok0.length, p1 - tok1.length).trim :: string.extractAll(tok0, tok1, p1)
      case None => Nil
    }

    @inline
    def findIndices(tok0: String, tok1: String): Option[(Int, Int)] = for {
      start <- string.indexOfOpt(tok0)
      end <- string.indexOfOpt(tok1, start)
    } yield (start, end)

    @inline
    def findIndices(tok0: String, tok1: String, fromIndex: Int): Option[(Int, Int)] = for {
      start <- string.indexOfOpt(tok0, fromIndex)
      end <- string.indexOfOpt(tok1, start)
    } yield (start, end)

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
    def isBlank: Boolean = string.trim.isEmpty

    @inline
    def nonBlank: Boolean = string.trim.nonEmpty

    @inline
    def isValidEmail: Boolean = string.contains("@") // TODO

    @inline
    def nonValidEmail: Boolean = !string.isValidEmail

    @inline
    def limitTo(length: Int): String = if(string.length > length) string.take(length) + "..." else string

    @inline
    def unquote: String = string match {
      case s if s.startsWith("\"") && s.endsWith("\"") => s.drop(1).dropRight(1)
      case s => s
    }

  }

}
