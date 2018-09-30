package com.qwery.platform.flink

import org.slf4j.LoggerFactory

/**
  * Flink Console
  * @author lawrence.daniels@gmail.com
  */
object FlinkConsole {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  /**
    * DEBUG statement
    * @example
    * {{{ DEBUG 'This is a debug message.' }}}
    * @param text the text to print
    */
  def debug(text: String): FlinkInvokable = new FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
      logger.debug(text)
      None
    }
  }

  /**
    * ERROR statement
    * @example
    * {{{ ERROR 'This is a informational message.' }}}
    * @param text the text to print
    */
  def error(text: String): FlinkInvokable = new FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
      logger.error(text)
      None
    }
  }

  /**
    * INFO statement
    * @example
    * {{{ INFO 'This is a informational message.' }}}
    * @param text the text to print
    */
  def info(text: String): FlinkInvokable = new FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
      logger.info(text)
      None
    }
  }

  /**
    * PRINT statement
    * @example
    * {{{ PRINT 'This is it!' }}}
    * @author lawrence.daniels@gmail.com
    */
  def print(text: String): FlinkInvokable = new FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
      println(text)
      None
    }
  }

  /**
    * WARN statement
    * @example
    * {{{ WARN 'This is a warning message.' }}}
    * @param text the text to print
    */
  def warn(text: String): FlinkInvokable = new FlinkInvokable {
    override def execute(input: Option[DataFrame])(implicit rc: FlinkQweryContext): Option[DataFrame] = {
      logger.warn(text)
      None
    }
  }

}
