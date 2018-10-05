package com.qwery.language

/**
  * Represents a token stream; a high-level abstraction of a [[TokenIterator token iterator]]
  * @author lawrence.daniels@gmail.com
  */
case class TokenStream(tokens: List[Token]) extends PeekableIterator[Token](tokens) {

  def apply(text: String): Boolean = peek.exists(_.text equalsIgnoreCase text)

  def expect(text: => String): this.type = {
    if (!nextOption.exists(_.is(text))) throw new SyntaxException(s"Expected keyword or symbol '$text'")
    this
  }

  def is(text: => String): Boolean = {
    if (text contains " ") {
      val words = text.trim.split("[ ]").map(_.trim).toSeq
      val mappings = words.zipWithIndex map { case (word, offset) => word -> peekAhead(offset) }
      mappings.forall { case (word, token) => token.exists(_ is word) }
    }
    else peek.exists(_.text equalsIgnoreCase text)
  }

  def isnt(text: => String): Boolean = !is(text)

  def isBackticks: Boolean = peek.exists {
    case t: QuotedToken => t.isBackTicks
    case _ => false
  }

  def isDoubleQuoted: Boolean = peek.exists {
    case t: QuotedToken => t.isDoubleQuoted
    case _ => false
  }

  def isNumeric: Boolean = peek.exists {
    case _: NumericToken => true
    case _ => false
  }

  def isQuoted: Boolean = isDoubleQuoted || isSingleQuoted

  def isSingleQuoted: Boolean = peek.exists {
    case t: QuotedToken => t.isSingleQuoted
    case _ => false
  }

  def isText: Boolean = peek.exists(_.isInstanceOf[AlphaToken])

  def matches(pattern: => String): Boolean = peek.exists(_.text.matches(pattern))

  def nextIf(keyword: => String): Boolean = {
    val result = is(keyword)
    if(result) skip(keyword.split("[ ]").length)
    result
  }

  def skip(count: Int): Unit = position = if (position + count < tokens.length) position + count else tokens.length

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