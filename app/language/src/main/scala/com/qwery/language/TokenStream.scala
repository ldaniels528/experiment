package com.qwery.language

/**
  * Represents a token stream; a high-level abstraction of a [[TokenIterator token iterator]]
  * @author lawrence.daniels@gmail.com
  */
case class TokenStream(tokens: List[Token]) extends PeekableIterator[Token](tokens) {

  /**
    * Returns true, if the given text (case insensitive) matches the next token in the stream
    * @param text the given text (e.g. "SELECT")
    * @return true, if the given text (case insensitive) matches the next token in the stream
    */
  def apply(text: String): Boolean = peek.exists(_.text equalsIgnoreCase text)

  /**
    * Facilitates processing of logical block statements; looping until the ending token
    * has been reached (e.g. stream => [ "(", ..., ")" ])
    * @param begin     the keyword or symbol to expect before processing
    * @param end       the keyword or symbol to expect after processing
    * @param delimiter the optional delimiter to use between iterations
    * @param block     the user-defined processing function
    * @return the list of [[A processed items]]
    */
  def capture[A](begin: String, end: String, delimiter: Option[String] = None)(block: TokenStream => A): List[A] = {
    var list: List[A] = Nil
    expect(begin)
    while (hasNext && (this isnt end)) {
      list = block(this) :: list
      delimiter.foreach(delim => if (this isnt end) expect(delim))
    }
    expect(end)
    list.reverse
  }

  /**
    * Facilitates optional processing of logical block statements; looping until the ending token
    * has been reached, then returns the list of captured elements (e.g. stream => [ "(", ..., ")" ])
    * @param begin     the keyword or symbol to expect before processing
    * @param end       the keyword or symbol to expect after processing
    * @param delimiter the optional delimiter to use between iterations
    * @param block     the user-defined processing function
    * @return the list of [[A processed items]]
    */
  def captureIf[A](begin: String, end: String, delimiter: Option[String] = None)(block: TokenStream => A): List[A] = {
    if (this is begin) capture(begin, end, delimiter)(block) else Nil
  }

  /**
    * Throws an exception if the next token(s) in the stream does not match the given text
    * @param text the given text (e.g. "SELECT * FROM")
    * @return a [[TokenStream self reference]]
    */
  def expect(text: => String): this.type = {
    val keywords = text.trim.split("[ ]")
    val tokens = (0 until keywords.length).flatMap(peekAhead)
    if (keywords.length != tokens.length || (keywords zip tokens).exists { case (keyword, token) => token.isnt(keyword) })
      throw SyntaxException(s"Expected keyword or symbol '$text'", this)
    else skip(keywords.length)
  }

  /**
    * Facilitates processing of sequential elements (e.g. stream => [ "(", ..., ")" ])
    * @param begin the keyword or symbol to expect before processing
    * @param end   the keyword or symbol to expect after processing
    * @param block the user-defined processing function
    * @return the [[A result]]
    */
  def extract[A](begin: String, end: String)(block: TokenStream => A): A = (expect(begin), block(this), expect(end))._2

  /**
    * Returns true, if the given text (case insensitive) matches the next token(s) in the stream
    * @param text the given text (e.g. "SELECT * FROM")
    * @return true, if the given text (case insensitive) matches the next token(s) in the stream
    */
  def is(text: => String): Boolean = {
    if (text contains " ") {
      val words = text.trim.split("[ ]").map(_.trim).toSeq
      val mappings = words.zipWithIndex map { case (word, offset) => word -> peekAhead(offset) }
      mappings.forall { case (word, token) => token.exists(_ is word) }
    }
    else peek.exists(_.text equalsIgnoreCase text)
  }

  /**
    * Indicates whether the next token is back-ticks quoted text
    * @return true, if the next token is back-ticks quoted text
    */
  def isBackticks: Boolean = peek.exists {
    case t: QuotedToken => t.isBackTicks
    case _ => false
  }

  /**
    * Indicates whether the next token is double-quoted text
    * @return true, if the next token is double-quoted text
    */
  def isDoubleQuoted: Boolean = peek.exists {
    case t: QuotedToken => t.isDoubleQuoted
    case _ => false
  }

  /**
    * Indicates whether the next token is numeric text
    * @return true, if the next token is numeric text
    */
  def isNumeric: Boolean = peek.exists {
    case _: NumericToken => true
    case _ => false
  }

  /**
    * The inverse of [[is()]]
    * @param text the given text (e.g. "SELECT * FROM")
    * @return true, if the given text (case insensitive) does not match the next token(s) in the stream
    */
  def isnt(text: => String): Boolean = !is(text)

  /**
    * Indicates whether the next token is back-ticks, double- or single-quoted text
    * @return true, if the next token is quoted text
    */
  def isQuoted: Boolean = isDoubleQuoted || isSingleQuoted

  /**
    * Indicates whether the next token is single-quoted text
    * @return true, if the next token is single-quoted text
    */
  def isSingleQuoted: Boolean = peek.exists {
    case t: QuotedToken => t.isSingleQuoted
    case _ => false
  }

  /**
    * Indicates whether the next token is alphanumeric text
    * @return true, if the next token is alphanumeric text
    */
  def isText: Boolean = peek.exists(_.isInstanceOf[AlphaToken])

  /**
    * Indicates whether the next token matches the given regular expression pattern
    * @param pattern the given regular expression pattern
    * @return true, if the regular expression pattern
    */
  def matches(pattern: => String): Boolean = peek.exists(_.text.matches(pattern))

  /**
    * Advances to the next token if it matches the given keyword
    * @param text the given keyword
    * @return true, if the next token if it matches the given keyword
    */
  def nextIf(text: => String): Boolean = {
    val result = is(text)
    if (result) skip(text.trim.split("[ ]").length)
    result
  }

  /**
    * Advances the given number of positions (up to the end of the stream)
    * @param count the given number of positions to advance
    */
  def skip(count: Int): this.type = {
    position = Math.min(position + count, tokens.length)
    this
  }

  override def toString: String = if (position < tokens.length) tokens.slice(position, tokens.length).mkString("|") else ""

}

/**
  * Token Stream Companion
  * @author lawrence.daniels@gmail.com
  */
object TokenStream {

  /**
    * Creates a new TokenStream instance
    * @param it the given token iterator
    * @return the [[TokenStream token stream]]
    */
  def apply(it: Iterator[Token]): TokenStream = new TokenStream(it.toList)

  /**
    * Creates a new TokenStream instance
    * @param query the given query string
    * @return the [[TokenStream token stream]]
    */
  def apply(query: String): TokenStream = apply(TokenIterator(query))

}