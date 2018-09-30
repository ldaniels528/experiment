package com.qwery.util

/**
  * Formatting Helper
  * @author lawrence.daniels@gmail.com
  */
object FormattingHelper {

  final implicit class SymbolFormatting(val values: Seq[Symbol]) extends AnyVal {

    def and(delimiter: String = ", "): String = formatted(delimiter, terminator = Some(" AND "))

    def or(delimiter: String = ", "): String = formatted(delimiter, terminator = Some(" OR "))

    def formatted(delimiter: String = ",", terminator: Option[String]): String = {
      val myValues = values.map(_.name)
      terminator match {
        case Some(term) if myValues.size > 1 => Seq(myValues.init.mkString(delimiter), myValues.last).mkString(term)
        case _ => myValues.mkString(delimiter)
      }
    }

  }

  final implicit class StringFormatting(val values: Seq[String]) extends AnyVal {

    def and(delimiter: String = ", "): String = formatted(delimiter, terminator = Some(" AND "))

    def or(delimiter: String = ", "): String = formatted(delimiter, terminator = Some(" OR "))

    def formatted(delimiter: String = ",", terminator: Option[String]): String = terminator match {
      case Some(term) if values.size > 1 => Seq(values.init.mkString(delimiter), values.last).mkString(term)
      case _ => values.mkString(delimiter)
    }

  }

}
