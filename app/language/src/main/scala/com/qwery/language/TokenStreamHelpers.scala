package com.qwery.language

/**
  * Token Stream Helpers
  * @author lawrence.daniels@gmail.com
  */
object TokenStreamHelpers {
  private val identifierRegEx = "[_a-zA-Z][_a-zA-Z0-9]{0,30}"

  /**
    * Token Extensions
    * @param token the given [[Token]]
    */
  final implicit class TokenExtensions[A <: Token](val token: A) extends AnyVal {

    @inline def isIdentifier: Boolean = token.matches(identifierRegEx)

  }

  /**
    * TokenStream Extensions
    * @param ts the given [[TokenStream]]
    */
  final implicit class TokenStreamExtensions(val ts: TokenStream) extends AnyVal {

    @inline def isConstant: Boolean = ts.isNumeric || ts.isQuoted

    @inline def isField: Boolean = ts.isBackticks || (ts.isIdentifier && !ts.isFunction)

    @inline def isFunction: Boolean =
      (for (a <- ts(0); b <- ts(1); _ <- ts(2)) yield a.isIdentifier && (b is "(")).contains(true)

    @inline def isIdentifier: Boolean = ts.peek.exists(_.isIdentifier)

    @inline def isJoinColumn: Boolean =
      (for (a <- ts(0); b <- ts(1); c <- ts(2)) yield a.isIdentifier && (b is ".") && c.isIdentifier).contains(true)

  }

}
