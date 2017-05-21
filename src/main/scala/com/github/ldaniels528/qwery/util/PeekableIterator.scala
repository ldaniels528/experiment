package com.github.ldaniels528.qwery.util

/**
  * Peekable Iterator
  * @author lawrence.daniels@gmail.com
  */
class PeekableIterator[T](values: Seq[T]) extends Iterator[T] {
  private var marks: List[Int] = Nil
  protected var position = 0

  override def hasNext: Boolean = position < values.length

  def mark(): Unit = marks = position :: marks

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
    val result = position match {
      case p if p >= values.length => Some(values.last)
      case p if p > 0 => Some(values(position))
      case _ => None
    }
    position -= 1
    result
  }

  def reset(): Boolean = marks.headOption exists { markedPos =>
    position = markedPos
    marks = marks.tail
    true
  }

  override def toString: String = s"PeekableIterator(${values.toList})"

}