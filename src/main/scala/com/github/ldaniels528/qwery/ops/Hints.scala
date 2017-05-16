package com.github.ldaniels528.qwery.ops

/**
  * Represents query hints
  * @author lawrence.daniels@gmail.com
  */
case class Hints(append: Boolean = true, delimiter: String = ",", headers: Boolean = true, quoted: Boolean = true)