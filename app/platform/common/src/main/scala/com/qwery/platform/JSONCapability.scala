package com.qwery.platform

import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Extraction.decompose

/**
  * JSON Capability
  * @author lawrence.daniels@gmail.com
  */
trait JSONCapability {

  def toJSON: String = {
    implicit val formats: DefaultFormats = DefaultFormats
    compactRender(decompose(this))
  }

  def toJSONPretty: String = {
    implicit val formats: DefaultFormats = DefaultFormats
    prettyRender(decompose(this))
  }

  override lazy val toString: String = toJSON

}

