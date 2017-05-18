package com.github.ldaniels528.qwery.codecs

/**
  * Represents a message/data CODEC
  * @author lawrence.daniels@gmail.com
  */
trait Codec[T] extends Encoder[T] with Decoder[T]