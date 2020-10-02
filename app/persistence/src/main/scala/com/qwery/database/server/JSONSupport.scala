package com.qwery.database.server

import net.liftweb.json.Extraction.decompose
import net.liftweb.json.{DefaultFormats, JValue, compactRender, parse, prettyRender}

/**
 * JSON Support Capability
 * @author lawrence.daniels@gmail.com
 */
object JSONSupport {
  implicit val formats: DefaultFormats = DefaultFormats

  final implicit class JSONStringConversion(val jsonString: String) extends AnyVal {
    @inline def fromJSON[T](implicit m: Manifest[T]): T = parse(jsonString).extract[T]

    @inline def toLiftJs: JValue = parse(jsonString)
  }

  final implicit class JSONProductConversion[T <: Product](val value: T) extends AnyVal {
    @inline def toJSON: String = compactRender(decompose(value))

    @inline def toJSONPretty: String = prettyRender(decompose(value))

    @inline def toLiftJs: JValue = decompose(value)
  }

}
