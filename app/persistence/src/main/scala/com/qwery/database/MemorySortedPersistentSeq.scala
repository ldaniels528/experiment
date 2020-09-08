package com.qwery.database

import com.qwery.database.MemorySortedPersistentSeq.BSTNode
import com.qwery.util.OptionHelper.OptionEnrichment

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Represents a Binary Search Tree (in memory implementation)
 * @param f the function which transforms the item into its comparable value
 * @tparam T the item/product type
 * @tparam V the value type
 */
class MemorySortedPersistentSeq[T <: Product : ClassTag, V <: Comparable[V]](f: T => V) extends SortedPersistentSeq[T, V](f) {
  private var root: BSTNode[T] = _

  override def add(item: T): Unit = {
    if (root == null) root = BSTNode(item) else attach(item, root)
  }

  override def ascending: Stream[T] = ascending(root)

  override def contains(item: T): Boolean = {
    var node = root
    val value = f(item)
    while (node != null) {
      value.compareTo(f(node.item)) match {
        case r if r < 0 => node = node.left
        case r if r > 0 => node = node.right
        case _ => return true
      }
    }
    false
  }

  override def descending: Stream[T] = descending(root)

  override def exists(f: T => Boolean): Boolean = {
    def recurse(node: BSTNode[T], f: T => Boolean): Boolean = if (node == null) false else {
      recurse(node.left, f)
      if (f(node.item)) return true
      recurse(node.right, f)
    }

    recurse(root, f)
  }

  override def foreach[U](f: T => U): Unit = {
    def recurse(node: BSTNode[T], f: T => U): Unit = if (node != null) {
      recurse(node.left, f)
      f(node.item)
      recurse(node.right, f)
    }

    recurse(root, f)
  }

  override def headOption: Option[T] = min(root)

  override def lastOption: Option[T] = max(root)

  override def map[U](f: T => U): Seq[U] = ascending(root).map(f)

  override def nthLargest(nth: Int): Option[T] = {
    val list = descending.take(nth)
    if (list.size < nth) None else list.lastOption
  }

  override def nthSmallest(nth: Int): Option[T] = {
    val list = ascending.take(nth)
    if (list.size < nth) None else list.lastOption
  }

  @tailrec
  private def attach(item: T, node: BSTNode[T]): Unit = {
    val value = f(item)
    value.compareTo(f(node.item)) match {
      case r if r < 0 => if (node.left != null) attach(item, node.left) else node.left = BSTNode(item)
      case r if r > 0 => if (node.right != null) attach(item, node.right) else node.right = BSTNode(item)
      case _ =>
    }
  }

  private def ascending(node: BSTNode[T]): Stream[T] = {
    if (node == null) Stream.empty else ascending(node.left) #::: node.item #:: ascending(node.right)
  }

  private def descending(node: BSTNode[T]): Stream[T] = {
    if (node == null) Stream.empty else descending(node.right) #::: node.item #:: descending(node.left)
  }

  private def max(node: BSTNode[T]): Option[T] = {
    if (node == null) None else max(node.right) ?? Option(node.item) ?? max(node.left)
  }

  private def min(node: BSTNode[T]): Option[T] = {
    if (node == null) None else min(node.left) ?? Option(node.item) ?? min(node.right)
  }

}

/**
 * MemoryBSTree Companion
 */
object MemorySortedPersistentSeq {

  case class BSTNode[T](item: T, var left: BSTNode[T] = null, var right: BSTNode[T] = null)

}