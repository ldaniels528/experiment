package com.github.ldaniels528.qwery.ops

import com.github.ldaniels528.qwery.util.StringHelper._

/**
  * Represents query hints
  * @author lawrence.daniels@gmail.com
  */
case class Hints(append: Boolean = true,
                 delimiter: String = ",",
                 headers: Boolean = true,
                 quoted: Boolean = true)
  extends SQLLike {

  override def toSQL: String = {
    s"HINTS(DELIMITER '$delimiter', HEADERS ${headers.onOff}, QUOTES ${quoted.onOff})"
  }

}
