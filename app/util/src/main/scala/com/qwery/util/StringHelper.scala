package com.qwery.util

import scala.language.reflectiveCalls
import scala.util.Try

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
    * Object Name Extension
    * @param obj the given [[AnyRef object]]
    */
  final implicit class ObjectNameExtension(val obj: AnyRef) extends AnyVal {

    @inline def getObjectFullName: String = obj.getClass.getName.replaceAllLiterally("$", "")

    @inline def getObjectSimpleName: String = obj.getClass.getSimpleName.replaceAllLiterally("$", "")

  }

  /**
    * StringLike Index Enrichment
    * @param string the given [[StringLike]]; e.g. a [[String]] or [[StringBuilder]]
    */
  final implicit class StringLikeIndexEnrichment[T <: StringLike](val string: T) extends AnyVal {

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

    @inline def noneIfBlank: Option[String] = string.trim match {
      case s if s.isEmpty => None
      case s => Option(s)
    }

    @inline def substringOpt(start: Int): Option[String] =
      if (start >= 0 && start < string.length) Option(string.substring(start)) else None


    @inline def substringOpt(start: Int, end: Int): Option[String] =
      if (start >= 0 && start <= end && end < string.length) Option(string.substring(start, end)) else None

    @inline def toSafeByte: Try[Byte] = Try(string.toByte)

    @inline def toSafeDouble: Try[Double] = Try(string.toDouble)

    @inline def toSafeFloat: Try[Float] = Try(string.toFloat)

    @inline def toSafeInt: Try[Int] = Try(string.toInt)

    @inline def toSafeLong: Try[Long] = Try(string.toLong)

    @inline def toSafeShort: Try[Short] = Try(string.toShort)

  }

}

