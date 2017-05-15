package com.github.ldaniels528.qwery.util

/**
  * String Helper
  * @author lawrence.daniels@gmail.com
  */
object StringHelper {

  final implicit class BooleanConversions(val state: Boolean) extends AnyVal {

    @inline
    def onOff: String = if (state) "ON" else "OFF"

  }

  /**
    * Delimited Text Enrichment
    * @param text the given Delimited text string
    */
  final implicit class DelimitedTextEnrichment(val text: String) extends AnyVal {

    def toSingleLine: String = text.replaceAllLiterally("\n", " ").replaceAllLiterally("  ", " ").trim

    def delimitedSplit(delimiter: Char): List[String] = {
      var inQuotes = false
      val sb = new StringBuilder()
      val values = text.toCharArray.foldLeft[List[String]](Nil) {
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

}
