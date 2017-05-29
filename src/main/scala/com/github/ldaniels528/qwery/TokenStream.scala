package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.util.PeekableIterator

/**
  * Represents a token stream; a high-level abstraction of a [[TokenIterator token iterator]]
  * @author lawrence.daniels@gmail.com
  */
class TokenStream(tokens: List[Token]) extends PeekableIterator[Token](tokens) {

  def apply(text: String): Boolean = peek.exists(_.text.equalsIgnoreCase(text))

  def die[A](message: String): A = throw SyntaxException(message, this)

  def dieEOS[A]: A = die("Unexpected end of statement")

  def expect(text: => String): this.type = {
    if (!nextOption.exists(_.is(text))) throw new SyntaxException(s"Expected $text")
    this
  }

  def is(text: => String): Boolean = {
    if (text.contains(" ")) {
      val words = text.trim.split("[ ]").map(_.trim)
      val mappings = words.zipWithIndex map { case (word, offset) => word -> peekAhead(offset) }
      mappings.forall { case (word, token) => token.exists(_.is(word)) }
    }
    else peek.exists(_.text.equalsIgnoreCase(text))
  }

  def matches(pattern: => String): Boolean = peek.exists(_.text.matches(pattern))

  def nextIf(keyword: => String): Boolean = is(keyword) && nextOption.nonEmpty

  def isBackticks: Boolean = peek.exists {
    case t: QuotedToken => t.isBackticks
    case _ => false
  }

  def isDoubleQuoted: Boolean = peek.exists {
    case t: QuotedToken => t.isDoubleQuoted
    case _ => false
  }

  def isNumeric: Boolean = peek.exists {
    case t: NumericToken => true
    case _ => false
  }

  def isQuoted: Boolean = isDoubleQuoted || isSingleQuoted

  def isSingleQuoted: Boolean = peek.exists {
    case t: QuotedToken => t.isSingleQuoted
    case _ => false
  }

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