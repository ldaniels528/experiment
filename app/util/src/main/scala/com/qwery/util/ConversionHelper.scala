package com.qwery.util

/**
  * Conversion Helper
  * @author lawrence.daniels@gmail.com
  */
object ConversionHelper {

  /**
    * Properties Conversion
    * @param values the [[Map]] to convert
    */
  final implicit class PropertiesConversion[V <: AnyRef](val values: Map[String, V]) extends AnyVal {

    def toProperties: java.util.Properties = {
      val props = new java.util.Properties()
      values foreach { case (key, value) => props.put(key, value) }
      props
    }

  }

}
