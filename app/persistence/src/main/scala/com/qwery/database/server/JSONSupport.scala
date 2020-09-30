package com.qwery.database.server

import net.liftweb.json.Extraction.decompose
import net.liftweb.json.{DefaultFormats, compactRender, prettyRender}

/**
 * JSON Support Companion
 * @author lawrence.daniels@gmail.com
 */
object JSONSupport {

  final implicit class JSONString(val jsonString: String) extends AnyVal {
    @inline
    def fromJSON[T](implicit m: Manifest[T]): T = {
      import net.liftweb.json.parse
      implicit val formats: DefaultFormats = DefaultFormats
      parse(jsonString).extract[T]
    }
  }

  final implicit class JSONProduct[T <: Product](val value: T) extends AnyVal {
    @inline
    def toJSON: String = {
      implicit val formats: DefaultFormats = DefaultFormats
      compactRender(decompose(value))
    }

    @inline
    def toJSONPretty: String = {
      implicit val formats: DefaultFormats = DefaultFormats
      prettyRender(decompose(value))
    }
  }

}
