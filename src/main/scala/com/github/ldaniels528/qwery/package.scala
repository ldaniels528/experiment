package com.github.ldaniels528

/**
  * qwery package object
  * @author lawrence.daniels@gmail.com
  */
package object qwery {

  type Column = (String, Any)

  type Row = Seq[Column]

  type ResultSet = TraversableOnce[Row]

}
