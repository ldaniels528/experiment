package com.qwery.util

import scala.language.reflectiveCalls

/**
  * String Helper
  * @author lawrence.daniels@gmail.com
  */
object StringHelper {

  type StringLike = {
    def indexOf(s: String): Int
    def indexOf(s: String, fromPos: Int): Int
    def lastIndexOf(s: String): Int
    def lastIndexOf(s: String, fromPos: Int): Int
  }

  @inline private def toOption(index: Int): Option[Int] = index match {
    case -1 => None
    case _ => Some(index)
  }

  /**
    * Index Enrichment
    * @param string the given [[StringLike]]; e.g. a [[String]] or [[StringBuilder]]
    */
  final implicit class IndexEnrichment[T <: StringLike](val string: T) extends AnyVal {

    @inline def indexOfOpt(s: String): Option[Int] = toOption(string.indexOf(s))

    @inline def indexOfOpt(s: String, fromPos: Int): Option[Int] = toOption(string.indexOf(s, fromPos))

    @inline def lastIndexOfOpt(s: String): Option[Int] = toOption(string.lastIndexOf(s))

    @inline def lastIndexOfOpt(s: String, fromPos: Int): Option[Int] = toOption(string.lastIndexOf(s, fromPos))

  }

  /**
    * String Enrichment
    * @param string the given [[String]]
    */
  final implicit class StringEnrichment(val string: String) extends AnyVal {

    def delimitedSplit(delimiter: Char): List[String] = {
      var inQuotes = false
      val sb = new StringBuilder()
      val values = string.toCharArray.foldLeft[List[String]](Nil) {
        case (list, ch) if ch == '"' =>
          inQuotes = !inQuotes
          //sb.append(ch)
          list
        case (list, ch) if inQuotes & ch == delimiter =>
          sb.append(ch); list
        case (list, ch) if !inQuotes & ch == delimiter =>
          val s = sb.toString().trim
          sb.clear()
          list ::: s :: Nil
        case (list, ch) =>
          sb.append(ch); list
      }
      if (sb.toString().trim.nonEmpty) values ::: sb.toString().trim :: Nil else values
    }

  }

  final implicit class ObjectNameExtension(val obj: AnyRef) extends AnyVal {

    @inline def getObjectFullName: String = obj.getClass.getName.replaceAllLiterally("$", "")

    @inline def getObjectSimpleName: String = obj.getClass.getSimpleName.replaceAllLiterally("$", "")

  }

}

