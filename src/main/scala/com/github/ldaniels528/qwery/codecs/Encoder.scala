package com.github.ldaniels528.qwery.codecs

import com.github.ldaniels528.qwery.ops.Row

/**
  * Represents a message/data encoder
  * @author lawrence.daniels@gmail.com
  */
trait Encoder[T] {

  def encode(row: Row): T

}
