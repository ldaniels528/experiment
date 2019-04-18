package com.qwery

import com.qwery.models.{Invokable, Queryable}
import com.qwery.util.FormattingHelper._

/**
  * language package object
  * @author lawrence.daniels@gmail.com
  */
package object language {

  /**
    * Token Stream Enrichment
    * @param ts the given [[TokenStream]]
    */
  final implicit class TokenStreamEnrichment(val ts: TokenStream) extends AnyVal {

    @inline def die[A](message: => String, cause: Throwable = null): A = throw SyntaxException(message, ts, cause)

    @inline def dieKeyword[A](symbols: Symbol*): A = {
      throw SyntaxException(s"Expected keyword ${symbols.or()} near '${ts.peek.map(_.text).orNull}'", ts)
    }

    @inline def dieKeyword[A](symbols: List[String]): A = {
      throw SyntaxException(s"Expected keyword ${symbols.or()} near '${ts.peek.map(_.text).orNull}'", ts)
    }

    @inline def decode(tuples: (String, TokenStream => Invokable)*): Invokable =
      decodeOpt(tuples: _*).getOrElse(ts.dieKeyword(tuples.map(_._1).toList.sortBy(x => x)))

    @inline def decodeQuery(tuples: (String, TokenStream => Queryable)*): Queryable =
      decodeOpt(tuples: _*).getOrElse(ts.dieKeyword(tuples.map(_._1).toList.sortBy(x => x)))

    @inline def decodeOpt[T](tuples: (String, TokenStream => T)*): Option[T] =
      for (fx <- tuples.find { case (name, _) => ts is name } map (_._2)) yield fx(ts)

  }

}
