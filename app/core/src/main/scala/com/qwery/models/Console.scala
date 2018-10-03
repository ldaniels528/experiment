package com.qwery.models

/**
  * The base trait for all console commands
  * @author lawrence.daniels@gmail.com
  */
sealed trait Console extends Invokable {
  def text: String
}

/**
  * Console Companion
  * @author lawrence.daniels@gmail.com
  */
object Console {

  /**
    * DEBUG statement
    * @example
    * {{{ DEBUG 'This is a debug message.' }}}
    * @param text the text to print
    */
  case class Debug(text: String) extends Console

  /**
    * ERROR statement
    * @example
    * {{{ ERROR 'This is a informational message.' }}}
    * @param text the text to print
    */
  case class Error(text: String) extends Console

  /**
    * INFO statement
    * @example
    * {{{ INFO 'This is a informational message.' }}}
    * @param text the text to print
    */
  case class Info(text: String) extends Console

  /**
    * LOG statement
    * @example
    * {{{ LOG 'This is a log message.' }}}
    * @param text the text to print
    */
  case class Log(text: String) extends Console

  /**
    * PRINT statement
    * @example
    * {{{ PRINT 'This is will be printed to STDOUT.' }}}
    * @param text the text to print
    */
  case class Print(text: String) extends Console

  /**
    * WARN statement
    * @example
    * {{{ WARN 'This is a warning message.' }}}
    * @param text the text to print
    */
  case class Warn(text: String) extends Console

}