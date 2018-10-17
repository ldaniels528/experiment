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
    cause match {
      case e: SyntaxException => new SyntaxException(message, e)
      case e =>
        val location = ts.peek.map(t => s"on line ${t.lineNo} at ${t.columnNo}").getOrElse("")
        val snippet = (0 to 3).flatMap(n => ts(n)).map(_.text).mkString(" ")
        new SyntaxException(s"$message near '$snippet' $location", e)
    }
  }

}
