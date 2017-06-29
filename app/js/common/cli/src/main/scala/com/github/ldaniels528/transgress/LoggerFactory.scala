package com.github.ldaniels528.transgress

import io.scalajs.nodejs.console

import scala.scalajs.js

/**
  * Logger Factory
  * @author lawrence.daniels@gmail.com
  */
object LoggerFactory {
  private val loggers = js.Dictionary[Logger]()

  def getLogger(`class`: Class[_]): Logger = getLogger(`class`.getSimpleName)

  def getLogger(className: String): Logger = loggers.getOrElseUpdate(className, new Logger(className))

  /**
    * Represents a logger
    * @author lawrence.daniels@gmail.com
    */
  class Logger(className: String) {

    @inline
    def log(format: String, args: Any*): Unit = console.log(s"$timestamp DEBUG [$className] $format", args: _*)

    @inline
    def info(format: String, args: Any*): Unit = console.info(s"$timestamp INFO  [$className] $format", args: _*)

    @inline
    def error(format: String, args: Any*): Unit = console.error(s"$timestamp ERROR [$className] $format", args: _*)

    @inline
    def error(format: js.Any): Unit = console.error(format)

    @inline
    def warn(format: String, args: Any*): Unit = console.warn(s"$timestamp WARN  [$className] $format", args: _*)

    @inline
    private def timestamp = {
      val date = new js.Date()
      s"%02d/%02d %02d:%02d:%02d".format(1 + date.getMonth(), date.getDate(), date.getHours(), date.getMinutes(), date.getSeconds())
    }

  }

}

