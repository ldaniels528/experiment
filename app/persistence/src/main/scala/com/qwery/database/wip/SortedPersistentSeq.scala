package com.qwery.database.wip

import scala.reflect.ClassTag

/**
 * Represents a Sorted Persistent Sequence
 * @param f the function which transforms the item into its comparable value
 * @tparam T the item/product type
 * @tparam V the value type
 */
abstract class SortedPersistentSeq[T <: Product : ClassTag, V <: Comparable[V]](f: T => V) extends Traversable[T] {

  def +=(item: T): Unit = add(item)

  def add(item: T): Unit

  def ascending: Stream[T]

  def contains(item: T): Boolean

  def descending: Stream[T]

  def foreach[U](f: T => U): Unit

  def headOption: Option[T]

  def lastOption: Option[T]

  def map[U](f: T => U): Seq[U]

  def nthLargest(nth: Int): Option[T]

  def nthSmallest(nth: Int): Option[T]

}

/**
 * BSTree Companion
 */
object SortedPersistentSeq {

  def apply[T <: Product : ClassTag, V <: Comparable[V]](f: T => V): SortedPersistentSeq[T,V] = new MemorySortedPersistentSeq[T,V](f)

}