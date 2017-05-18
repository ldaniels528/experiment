package com.github.ldaniels528.qwery.codecs

import com.github.ldaniels528.qwery.ops.Row

import scala.util.Try

/**
  * Represents a message/data decoder
  * @author lawrence.daniels@gmail.com
  */
trait Decoder[T] {

  def decode(bytes: T): Try[Row]

}
