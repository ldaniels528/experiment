package com.github.ldaniels528.qwery.util

import com.github.ldaniels528.qwery.util.OptionHelper._

/**
  * Properties Helper
  * @author Lawrence Daniels <lawrence.daniels@gmail.com>
  */
object PropertiesHelper {

  type JProperties = java.util.Properties

  /**
    * Properties Enrichment
    * @param props the given [[JProperties properties]]
    */
  implicit class PropertiesEnrichment(val props: JProperties) extends AnyVal {

    @inline
    def require(key: String): String = {
      Option(props.getProperty(key)).orDie(s"Required property '$key' is missing")
    }

  }

}
