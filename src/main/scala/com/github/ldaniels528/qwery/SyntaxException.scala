package com.github.ldaniels528.qwery

/**
  * Syntax Exception
  * @author lawrence.daniels@gmail.com
  */
class SyntaxException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/**
  * Syntax Exception
  * @author lawrence.daniels@gmail.com
  */
object SyntaxException {

  def apply(message: String, ts: TokenStream): SyntaxException = apply(message, ts, cause = null)

  def apply(message: String, ts: TokenStream, cause: Throwable): SyntaxException = {
    val position = ts.peek.map(_.start).getOrElse(-1)
    new SyntaxException(s"$message near '${ts.take(3).map(_.text).mkString(" ")}' at $position")
  }

  def apply(message: String, token: Token): SyntaxException = apply(message, token, cause = null)

  def apply(message: String, token: Token, cause: Throwable): SyntaxException = {
    new SyntaxException(s"$message near '${token.text}' at ${token.start}", cause)
  }

}