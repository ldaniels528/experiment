package com.qwery.platform

class PlatformPrint[T](text: String) extends PlatformInvokable[T] {
  def execute(input: Option[T]): Option[T] = {
    println(text)
    None
  }
}