package com.qwery.language

/**
  * Creates a new Syntax Exception
  * @param message the given failure message
  * @param cause   the given [[Throwable]]
  * @author lawrence.daniels@gmail.com
  */
class SyntaxException(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

/**
  * Syntax Exception Companion
  * @author lawrence.daniels@gmail.com
  */
object SyntaxException {

  /**
    * Creates a new Syntax Exception
    * @param message the given failure message
    * @param ts      the given [[TokenStream]]
    * @param cause   the given [[Throwable]]
    * @author lawrence.daniels@gmail.com
    */
  def apply(message: String, ts: TokenStream, cause: Throwable = null): SyntaxException = {
    val xts = ts.copy()
    val position = xts.peek.map(_.start).getOrElse(-1)
    cause match {
      case e: SyntaxException => new SyntaxException(message, e)
      case e => new SyntaxException(s"$message near '${xts.take(3).map(_.text).mkString(" ")}' at $position", e)
    }
  }

}
