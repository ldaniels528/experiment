package com.qwery.database.server

import net.liftweb.json.Extraction.decompose
import net.liftweb.json.{DefaultFormats, compactRender, prettyRender}

/**
 * JSON Support Trait
 * @author lawrence.daniels@coxautoinc.com
 */
trait JSONSupport {

  def toJSON: String = {
    implicit val formats: DefaultFormats = DefaultFormats
    compactRender(decompose(this))
  }

  def toJSONPretty: String = {
    implicit val formats: DefaultFormats = DefaultFormats
    prettyRender(decompose(this))
  }

}

/**
 * JSON Support Companion Trait
 * @author lawrence.daniels@coxautoinc.com
 */
trait JSONSupportCompanion[T <: JSONSupport] {

  def fromBytes(jsonBinary: Array[Byte])(implicit m: Manifest[T]): T = {
    import net.liftweb.json.parse
    implicit val formats: DefaultFormats = DefaultFormats
    parse(new String(jsonBinary, "UTF-8")).extract[T]
  }

  def fromString(jsonString: String)(implicit m: Manifest[T]): T = {
    import net.liftweb.json.parse
    implicit val formats: DefaultFormats = DefaultFormats
    parse(jsonString).extract[T]
  }

}
