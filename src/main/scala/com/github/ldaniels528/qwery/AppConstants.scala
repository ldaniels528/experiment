package com.github.ldaniels528.qwery

/**
  * Application Constants
  * @author lawrence.daniels@gmail.com
  */
object AppConstants {
  val Version = "0.3.4"
  val envHome = "QWERY_HOME"

  def welcome(module: String): String = {
    s"""
       | Qwery $module v$Version
       |         ,,,,,
       |         (o o)
       |-----oOOo-(_)-oOOo-----
      """.stripMargin
  }

}
