package com.qwery.util

/**
  * String Helper
  * @author lawrence.daniels@gmail.com
  */
object StringHelper {

  @inline private def toOption(index: Int): Option[Int] = index match {
    case -1 => None
    case _ => Some(index)
  }

  /**
    * String Builder Enrichment
    * @param sb the given [[StringBuilder]]
    */
  final implicit class StringBuilderEnrichment(val sb: StringBuilder) extends AnyVal {

    @inline def indexOfOpt(c: Char): Option[Int] = toOption(sb.indexOf(c))

    @inline def indexOfOpt(c: Char, fromPos: Int): Option[Int] = toOption(sb.indexOf(c, fromPos))

    @inline def indexOfOpt(s: String): Option[Int] = toOption(sb.indexOf(s))

    @inline def indexOfOpt(s: String, fromPos: Int): Option[Int] = toOption(sb.indexOf(s, fromPos))

  }

  /**
    * String Enrichment
    * @param text the given [[String]]
    */
  final implicit class StringEnrichment(val text: String) extends AnyVal {

    @inline def indexOfOpt(c: Char): Option[Int] = toOption(text.indexOf(c))

    @inline def indexOfOpt(c: Char, fromPos: Int): Option[Int] = toOption(text.indexOf(c, fromPos))

    @inline def indexOfOpt(s: String): Option[Int] = toOption(text.indexOf(s))

    @inline def indexOfOpt(s: String, fromPos: Int): Option[Int] = toOption(text.indexOf(s, fromPos))

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

