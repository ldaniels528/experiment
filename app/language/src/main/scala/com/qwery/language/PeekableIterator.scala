package com.qwery.language

/**
  * Peekable Iterator
  * @author lawrence.daniels@gmail.com
  */
class PeekableIterator[T](values: Seq[T], protected var position: Int = 0) extends Iterator[T] {
  private var marks: List[Int] = Nil

  def apply(offset: Int): Option[T] = peekAhead(offset)

  def discard(): Boolean = marks.headOption exists { _ =>
    marks = marks.tail
    true
  }

  override def hasNext: Boolean = position < values.length

  /**
    * Return the index (or position) within the iterator
    * @return the index (or position)
    */
  def index: Int = position

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

  def peek: Option[T] = if (hasNext) Option(values(position)) else None

  def peekAhead(offset: Int): Option[T] =
    if (position + offset >= 0 && position + offset < values.length) Option(values(position + offset)) else None

  def reset(): Boolean = marks.headOption exists { markedPos =>
    position = markedPos
    marks = marks.tail
    true
  }

  override def toString: String = s"PeekableIterator(${
    values.zipWithIndex.map {
      case (item, n) if n == position => s"[$item]"
      case (item, _) => item
    } mkString ", "
  })"

}
