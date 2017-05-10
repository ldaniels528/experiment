package com.github.ldaniels528.qwery

import com.github.ldaniels528.qwery.util.PeekableIterator

/**
  * Represents a token stream; a high-level abstraction of a [[TokenIterator token iterator]]
  * @author lawrence.daniels@gmail.com
  */
class TokenStream(tokens: List[Token]) extends PeekableIterator[Token](tokens) {

  def expect(text: String): Unit = {
    if (!nextOption.exists(_.is(text))) throw new SyntaxException(s"Expected $text")
  }

  def is(text: String): Boolean = peek.exists(_.text.equalsIgnoreCase(text))

  def matches(pattern: String): Boolean = peek.exists(_.text.matches(pattern))

  def nextIf(keyword: String): Boolean = is(keyword) && nextOption.nonEmpty

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