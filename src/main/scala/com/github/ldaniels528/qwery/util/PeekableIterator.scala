package com.github.ldaniels528.qwery.util

/**
  * Peekable Iterator
  * @author lawrence.daniels@gmail.com
  */
class PeekableIterator[T](values: Seq[T]) extends Iterator[T] {
  protected var position = 0

  override def hasNext: Boolean = position < values.length

  override def next(): T = {
    // must have more elements
    if (!hasNext)
      throw new IllegalStateException("Out of bounds")

    // return the value
    val value = values(position)
    position += 1
    value
  }

  def nextOption: Option[T] = {
    if (hasNext) {
      val value = Some(values(position))
      position += 1
      value
    } else None
  }

  def peek: Option[T] = if (hasNext) Some(values(position)) else None

  def peekAhead(offset: Int): Option[T] = if (position + offset < values.length) Some(values(position + offset)) else None

  def previous: Option[T] = {
    val ok = position > 0
    if (ok) {
      val value = values(position)
      position -= 1
      Some(value)
    }
    else None
  }

}