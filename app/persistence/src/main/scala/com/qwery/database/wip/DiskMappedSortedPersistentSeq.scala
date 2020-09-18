package com.qwery.database.wip

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

import com.qwery.database._
import com.qwery.database.wip.DiskMappedSortedPersistentSeq.BSTNode
import com.qwery.util.OptionHelper.OptionEnrichment

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Represents a disk-mapped Binary Search Tree
 * @param f the function which transforms the item into its comparable value
 * @tparam T the item/product type
 * @tparam V the value type
 */
class DiskMappedSortedPersistentSeq[T <: Product : ClassTag, V <: Comparable[V]](persistenceFile: File)(f: T => V)
  extends SortedPersistentSeq[T, V](f) {
  import DiskMappedSortedPersistentSeq.Null
  private val device = new FileBlockDevice(columns = Nil, persistenceFile)

  val (columns, _) = PersistentSeq.toColumns[T]

  override def add(item: T): Unit = {
    if (length != 0) attach(item, getNode(offset = 0)) else writeNode(length, BSTNode[T](offset = 0, item))
  }

  override def ascending: Stream[T] = if (nonEmpty) sortAscending() else Stream.empty

  override def contains(item: T): Boolean = {
    var node = getNode(offset = 0)
    val value = f(item)
    while (node != null) {
      value.compareTo(f(node.item)) match {
        case r if r < 0 => node = if(node.left != Null) getNode(node.left) else null
        case r if r > 0 => node = if(node.right != Null) getNode(node.right) else null
        case _ => return true
      }
    }
    false
  }

  override def descending: Stream[T] = if (nonEmpty) sortDescending() else Stream.empty

  override def foreach[U](f: T => U): Unit = {
    def recurse(offset: ROWID, f: T => U): Unit = {
      val node_? = readNode(offset)
      if (node_?.nonEmpty) {
        node_?.foreach(node => recurse(node.left, f))
        node_?.foreach(node => f(node.item))
        node_?.foreach(node =>recurse(node.right, f))
      }
    }
    recurse(offset = 0, f)
  }

  override def headOption: Option[T] = min(rowID = 0)

  override def lastOption: Option[T] = max(rowID = 0)

  def length: ROWID = device.length

  override def map[U](f: T => U): Seq[U] = sortAscending().map(f)

  override def nthLargest(nth: Int): Option[T] = {
    val list = descending.take(nth)
    if (list.size < nth) None else list.lastOption
  }

  override def nthSmallest(nth: Int): Option[T] = {
    val list = ascending.take(nth)
    if (list.size < nth) None else list.lastOption
  }

  private def sortAscending(offset: ROWID = 0): Stream[T] = {
    if (offset == Null) Stream.empty else {
      val node = getNode(offset)
      sortAscending(node.left) #::: node.item #:: sortAscending(node.right)
    }
  }

  private def sortDescending(offset: ROWID = 0): Stream[T] = {
    if (offset == Null) Stream.empty else {
      val node = getNode(offset)
      sortAscending(node.right) #::: node.item #:: sortAscending(node.left)
    }
  }

  @tailrec
  private def attach(item: T, node: BSTNode[T]): Unit = {
    val value = f(item)
    value.compareTo(f(node.item)) match {
      case r if r < 0 => if (node.left != Null) attach(item, getNode(node.left)) else linkNode(node, item, isLeft = true)
      case r if r > 0 => if (node.right != Null) attach(item, getNode(node.right)) else linkNode(node, item, isLeft = false)
      case _ =>
    }
  }

  private def getNode(offset: ROWID): BSTNode[T] = {
    val bytes = device.readBlock(offset).array()
    toNode(offset, bytes, evenDeletes = true).getOrElse(throw new IllegalStateException(s"Node $offset not found"))
  }

  private def linkNode(node: BSTNode[T], item: T, isLeft: Boolean): Unit = {
    writeNode(node.offset, node = if (isLeft) node.copy(left = createNode(item)) else node.copy(right = createNode(item)))
  }

  private def createNode(item: T): ROWID = {
    val offset = length
    writeNode(offset, BSTNode[T](offset, item))
    offset
  }

  private def max(rowID: ROWID): Option[T] = {
    val node_? = readNode(rowID)
    if (node_?.isEmpty) None else node_?.flatMap(node => max(node.right)) ?? node_?.map(_.item) ?? node_?.flatMap(node => max(node.left))
  }

  private def min(rowID: ROWID): Option[T] = {
    val node_? = readNode(rowID)
    if (node_?.isEmpty) None else node_?.flatMap(node => max(node.left)) ?? node_?.map(_.item) ?? node_?.flatMap(node => max(node.right))
  }

  private def readNode(rowID: ROWID): Option[BSTNode[T]] = {
    val bytes = device.readBlock(rowID).array()
    toNode(rowID, bytes)
  }

  private def writeNode(rowID: ROWID, node: BSTNode[T]): Unit = {
    device.writeBlock(rowID, wrap(toBytes(node)))
  }

  private def toBytes(node: BSTNode[T]): Array[Byte] = {
    val buf = allocate(device.recordSize)
    //buf.put(toBytes(node.item))
    buf.position(buf.capacity() - 2 * LONG_BYTES)
    buf.putLong(node.left)
    buf.putLong(node.right)
    buf.array()
  }

  private def toNode(rowID: ROWID, bytes: Array[Byte], evenDeletes: Boolean = false): Option[BSTNode[T]] = {
    val buf = wrap(bytes)
    val rmd = buf.getRowMetaData
    if (rmd.isDeleted && !evenDeletes) None
    else {
      val left = buf.getLong().toURID
      val right = buf.getLong().toURID
      toItem(rowID, buf).map(item => BSTNode(rowID, item, left = left, right = right))
    }
  }

  def toItem(rowID: ROWID, buf: ByteBuffer, evenDeletes: Boolean = false): Option[T] = ???

}

/**
 * Disk-mapped Binary Search Tree
 */
object DiskMappedSortedPersistentSeq {
  private val Null = -1

  case class BSTNode[T](offset: ROWID, item: T, left: ROWID = Null, right: ROWID = Null)

}