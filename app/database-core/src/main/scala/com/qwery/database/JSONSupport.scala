package com.qwery.database

import spray.json._

/**
 * JSON Support Capability
 * @author lawrence.daniels@gmail.com
 */
object JSONSupport {

  final implicit class JSONStringConversion(val jsonString: String) extends AnyVal {
    @inline def fromJSON[T](implicit reader: JsonReader[T]): T = jsonString.parseJson.convertTo[T]
  }

  final implicit class JSONProductConversion[T](val value: T) extends AnyVal {

    @inline def toJSON(implicit writer: JsonWriter[T]): String = value.toJson.compactPrint

    @inline def toJSONPretty(implicit writer: JsonWriter[T]): String = value.toJson.prettyPrint

  }

}
