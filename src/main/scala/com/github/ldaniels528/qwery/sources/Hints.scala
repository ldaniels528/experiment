package com.github.ldaniels528.qwery.sources

/**
  * Represents a hints
  * @author lawrence.daniels@gmail.com
  */
case class Hints(append: Boolean = false,
                 delimiter: String = ",",
                 includeHeaders: Boolean = true,
                 quoted: Boolean = true) {

  override def toString = s"append = $append, delimiter = '$delimiter', includeHeaders = $includeHeaders, quoted = $quoted"

}
