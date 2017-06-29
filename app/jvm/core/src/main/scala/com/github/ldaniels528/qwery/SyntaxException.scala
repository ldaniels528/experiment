package com.github.ldaniels528.qwery

/**
  * Syntax Exception
  * @author lawrence.daniels@gmail.com
  */
class SyntaxException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/**
  * Syntax Exception Companion
  * @author lawrence.daniels@gmail.com
  */
object SyntaxException {

  def apply(message: String, ts: TokenStream, cause: Throwable = null): SyntaxException = {
    val xts = ts.makeCopy
    val position = xts.peek.map(_.start).getOrElse(-1)
    new SyntaxException(s"$message near '${xts.take(3).map(_.text).mkString(" ")}' at $position", cause)
  }

}