package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Represents query hints
  * @author lawrence.daniels@gmail.com
  */
case class Hints(append: Boolean = false,
                 delimiter: String = ",",
                 headers: Boolean = true,
                 quoted: Boolean = true) {

  override def toString: String = {
    s"WITH HINTS(APPEND ${append.onOff}, DELIMITER '$delimiter', HEADERS ${headers.onOff}, QUOTES ${quoted.onOff})"
  }

}
