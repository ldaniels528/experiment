package com.qwery.models

/**
  * SHOW statement
  * @example
  * {{{
  * SHOW @results
  * SHOW @results LIMIT 25
  * }}}
  * @author lawrence.daniels@gmail.com
  */
case class Show(rows: Invokable, limit: Option[Int] = None) extends Invokable