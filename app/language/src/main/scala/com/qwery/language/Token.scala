package com.qwery.language

/**
  * Represents a token
  * @author lawrence.daniels@gmail.com
  */
sealed trait Token {

  /**
    * Indicates whether the given text matches the current token
    * @param value the given text value
    * @return true, if the value matches the current token
    */
  def is(value: String): Boolean = text equalsIgnoreCase value

  /**
    * Indicates whether the given text does not match the current token
    * @param value the given text value
    * @return true, if the value matches the current token
    */
  def isnt(value: String): Boolean = !is(text)

  /**
    * @return the line number of this token
    */
  def lineNo: Int

  /**
    * @return the column number of this token
    */
  def columnNo: Int

  /**
    * Indicates whether the underlying text matches the given pattern
    * @param pattern the given pattern
    * @return true, if the underlying text matches the given pattern
    */
  def matches(pattern: String): Boolean = text matches pattern

  /**
    * @return the text contained by this token
    */
  def text: String

  /**
    * @return the typed value contained by this token
    */
  def value: Any

}

/**
  * Represents a text token
  * @author lawrence.daniels@gmail.com
  */
sealed trait TextToken extends Token {
  override def value: String
}

/**
  * Represents an alphanumeric token
  * @author lawrence.daniels@gmail.com
  */
case class AlphaToken(text: String, lineNo: Int, columnNo: Int) extends TextToken {
  override def value: String = text
}

/**
  * Represents a quoted token
  * @author lawrence.daniels@gmail.com
  */
case class QuotedToken(text: String, lineNo: Int, columnNo: Int, quoteChar: Char) extends TextToken {
  override def value: String = text

  def isBackTicks: Boolean = quoteChar == '`'

  def isDoubleQuoted: Boolean = quoteChar == '"'

  def isSingleQuoted: Boolean = quoteChar == '\''
}

/**
  * Represents a numeric token
  * @author lawrence.daniels@gmail.com
  */
case class NumericToken(text: String, lineNo: Int, columnNo: Int) extends Token {
  override def value: Double = text.toDouble
}

/**
  * Represents an operator token
  * @author lawrence.daniels@gmail.com
  */
case class OperatorToken(text: String, lineNo: Int, columnNo: Int) extends Token {
  override def value: String = text
}

/**
  * Represents a symbolic token
  * @author lawrence.daniels@gmail.com
  */
case class SymbolToken(text: String, lineNo: Int, columnNo: Int) extends Token {
  override def value: String = text
}

