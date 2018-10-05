package com.qwery.language

/**
  * Token Iterator
  * @author lawrence.daniels@gmail.com
  */
case class TokenIterator(input: String) extends Iterator[Token] {
  private var pos = 0
  private val ca = input.toCharArray
  private val operators = "=*-+/|&><".toCharArray
  private val compoundOperators = Seq("!=", ">=", "<=", "<>", "||", "**")

  private def parsers = List(
    parseNumeric _, parseAlphaNumeric _, parseQuotesBackticks _, parseQuotesDouble _,
    parseQuotesSingle _, parseCompoundOperators _, parseOperators _, parseSymbols _)

  override def hasNext: Boolean = {
    var last: Int = 0
    do {
      last = pos
      skipComments(startCh = "/*".toCharArray, endCh = "*/".toCharArray)
      skipComments(startCh = "--".toCharArray, endCh = "\n".toCharArray)
      skipComments(startCh = "//".toCharArray, endCh = "\n".toCharArray)
      skipWhitespace()
    } while (last != pos)
    pos < ca.length
  }

  override def next(): Token = {
    if (!hasNext) throw new NoSuchElementException()
    else {
      val outcome = parsers.foldLeft[Option[Token]](None) { (token_?, parser) =>
        if (token_?.isEmpty) parser() else token_?
      }
      outcome.getOrElse(throw new IllegalArgumentException(String.copyValueOf(ca, pos, ca.length)))
    }
  }

  def nextOption(): Option[Token] = {
    if (!hasNext) None
    else parsers.foldLeft[Option[Token]](None) { (token_?, parser) =>
      if (token_?.isEmpty) parser() else token_?
    }
  }

  def peek: Option[Token] = {
    val mark = pos
    val token_? = nextOption()
    pos = mark
    token_?
  }

  def span(length: Int): Option[String] = {
    if (pos + length < ca.length) Some(String.copyValueOf(ca, pos, length)) else None
  }

  @inline
  private def hasMore: Boolean = pos < ca.length

  private def parseAlphaNumeric(): Option[Token] = {
    val start = pos
    while (hasMore && (ca(pos).isLetterOrDigit || ca(pos) == '_')) pos += 1
    if (pos > start) Some(AlphaToken(String.copyValueOf(ca, start, pos - start), start)) else None
  }

  private def parseCompoundOperators(): Option[Token] = {
    if (hasMore && span(2).exists(compoundOperators.contains)) {
      val start = pos
      val result = span(2).map(OperatorToken(_, start))
      pos += 2
      result
    }
    else None
  }

  private def parseNumeric(): Option[Token] = {
    val start = pos
    while (hasMore && (ca(pos).isDigit || (ca.length > pos + 1 && ca(pos) == '.' && ca(pos + 1).isDigit))) pos += 1
    if (pos > start) Some(NumericToken(String.copyValueOf(ca, start, pos - start), start)) else None
  }

  private def parseOperators(): Option[Token] = {
    if (hasMore && operators.contains(ca(pos))) {
      val start = pos
      pos += 1
      Some(OperatorToken(ca(start).toString, start))
    }
    else None
  }

  private def parseQuotesBackticks() = parseQuotes('`')

  private def parseQuotesDouble() = parseQuotes('"')

  private def parseQuotesSingle() = parseQuotes('\'')

  private def parseQuotes(ch: Char): Option[Token] = {
    if (hasMore && ca(pos) == ch) {
      pos += 1
      val start = pos
      while (hasMore && ca(pos) != ch) pos += 1
      val length = pos - start
      pos += 1
      Some(QuotedToken(String.copyValueOf(ca, start, length), start, ch))
    }
    else None
  }

  private def parseSymbols(): Option[Token] = {
    if (hasMore) {
      val start = pos
      pos += 1
      Some(SymbolToken(ca(start).toString, start))
    }
    else None
  }

  private def skipComments(startCh: Array[Char], endCh: Array[Char]): Unit = {

    def matches(chars: Array[Char]): Boolean = pos + (chars.length - 1) < ca.length &&
      chars.zipWithIndex.forall { case (ch, offset) => ch == ca(pos + offset) }

    if (matches(startCh)) {
      pos += startCh.length
      while (hasMore && !matches(endCh)) pos += 1
      pos += endCh.length
    }
  }

  private def skipWhitespace(): Unit = while (hasMore && ca(pos).isWhitespace) pos += 1

}
