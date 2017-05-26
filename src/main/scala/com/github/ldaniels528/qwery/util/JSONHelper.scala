package com.github.ldaniels528.qwery.util

import java.util.{Properties => JProperties}

import net.liftweb.json.{JObject, JValue, parse}

import scala.io.Source

/**
  * JSON Helper
  * @author lawrence.daniels@gmail.com
  */
object JSONHelper {

  def getProperties(path: String): JProperties = {
    val js = parse(Source.fromFile(path).mkString)
    val props = new JProperties()
    js.values
    props
  }

  def unwrap(jv: JValue) = {
    jv match {
      case jo: JObject =>
      case jv =>
    }
  }

}
