package com.qwery.database

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import com.qwery.database.ItemConversion._
import com.qwery.database.PersistentSeq.{PersistenceIterator, RowWithMetadata}
import com.qwery.util.ResourceHelper._

import scala.io.Source
import scala.reflect.ClassTag

/**
 * Represents a persistent sequential collection
 */
abstract class PersistentSeq[T <: Product : ClassTag]() extends ItemConversion[T] with Traversable[T] {

  /**
   * Creates a concatenated copy of this sequence combined with the other sequence
   * @param traversable the other [[Traversable]]
   * @return a new [[PersistentSeq]]
   */
  def ++(traversable: Traversable[T]): PersistentSeq[T] = {
    val out = newDocument[T]()
    traversable match {
      case that: Seq[T] =>
        this.copyTo(out, fromPos = 0, toPos = this.length)
        writeBlocks(toBlocks(length, that))
      case that: PersistentSeq[T] =>
        this.copyTo(out, fromPos = 0, toPos = this.length)
        that.copyTo(out, fromPos = 0, toPos = that.length)
      case that =>
        this.copyTo(out, fromPos = 0, toPos = this.length)
        writeBlocks(toBlocks(out.length, that))
    }
    out
  }

  /**
   * Appends a collection of items to the end of this collection
   * @param items the collection of [[T items]] to append
   */
  def ++=(items: Seq[T]): Unit = append(items)

  /**
   * Appends an item to the end of this collection
   * @param item the [[T item]] to append
   */
  def +=(item: T): Unit = append(item)

  /**
   * Appends an item to the end of the file
   * @param item the [[T item]] to append
   * @return [[PersistentSeq self]]
   */
  def append(item: T): PersistentSeq[T] = writeBytes(length, toBytes(item))

  /**
   * Appends the collection of items to the end of the file
   * @param items the collection of [[T item]] to append
   * @return [[PersistentSeq self]]
   */
  def append(items: Traversable[T]): PersistentSeq[T] = writeBlocks(toBlocks(length, items))

  /**
   * Retrieves the item corresponding to the record offset
   * @param offset the record offset
   * @return the [[T item]]
   */
  def apply(offset: URID): T = {
    toItem(offset, buf = readBlock(offset), evenDeletes = true)
      .getOrElse(throw new IllegalStateException("No record found"))
  }

  /**
   * Computes the average of a column
   * @param predicate the search function
   * @return the [[Double average]]
   */
  def avg(predicate: T => Double): Double = {
    var (count, total) = (0d, 0d)
    _traverse(() => false) { item => total += predicate(item); count += 1 }
    if (count != 0) total / count else Double.NaN
  }

  /**
   * Truncates the collection; removing all items
   */
  def clear(): PersistentSeq[T] = shrinkTo(newSize = 0)

  /**
   * Closes the underlying file handle
   */
  def close(): Unit

  def collect[U <: Product : ClassTag](predicate: PartialFunction[T, U]): PersistentSeq[U] = {
    val that = newDocument[U]()
    _traverse(() => false) { item => that += predicate(item) }
    that
  }

  def collectFirst[U <: Product : ClassTag](predicate: PartialFunction[T, U]): Option[U] = {
    var item_? : Option[U] = None
    _traverse(() => item_?.nonEmpty) { item => item_? = Option(predicate(item)) }
    item_?
  }

  def compact(): PersistentSeq[T] = {
    trim()
    var eof = length - 1
    var offset: URID = 0
    while (offset < eof) {
      if (getRowMetaData(offset).isDeleted) {
        writeBytes(offset, readBytes(eof))
        setRowMetaData(eof, RowMetaData(isActive = false))
        eof -= 1
      }
      offset += 1
    }
    shrinkTo(eof + 1)
  }

  def contains(elem: T): Boolean = indexOfOpt(elem).nonEmpty

  def copyTo(that: PersistentSeq[T], fromPos: URID, toPos: URID): Unit = {
    that.writeBytes(that.length, readBytes(fromPos, numberOfBlocks = toPos - fromPos))
  }

  override def copyToArray[B >: T](array: Array[B], start: URID, len: URID): Unit = {
    val bytes = readBytes(start, len)
    for {
      n <- (0 until len).toArray
      index = n * recordSize
      item <- toItem(id = start + n, buf = wrap(bytes, index, recordSize))
    } array(n) = item
  }

  /**
   * Counts all active rows
   * @return the number of active rows
   * @see [[countRows]]
   */
  def count(): Int = countRows(_.isActive)

  /**
   * Counts the number of items matching the predicate
   * @param predicate the function defining which items should be included
   * @return the number of rows matching the predicate
   */
  override def count(predicate: T => Boolean): Int = {
    var counted: Int = 0
    _traverse(() => false) { item => if (predicate(item)) counted += 1 }
    counted
  }

  /**
   * Counts the number of rows matching the predicate
   * @param predicate the function defining which items should be included
   * @return the number of rows matching the predicate
   */
  def countRows(predicate: RowMetaData => Boolean): Int = {
    val eof = length
    var (offset, total) = (0, 0)
    while (offset < eof) {
      if (predicate(getRowMetaData(offset))) total += 1
      offset += 1
    }
    total
  }

  override def filter(predicate: T => Boolean): PersistentSeq[T] = {
    val that = newDocument[T]()
    _traverse(() => false) { item => if (predicate(item)) that += item }
    that
  }

  override def filterNot(predicate: T => Boolean): PersistentSeq[T] = {
    val that = newDocument[T]()
    _traverse(() => false) { item => if (!predicate(item)) that += item }
    that
  }

  def flatMap[U <: Product : ClassTag](predicate: T => TraversableOnce[U]): PersistentSeq[U] = {
    val that = newDocument[U]()
    _traverse(() => false) { item => predicate(item).foreach(that.append) }
    that
  }

  def foreach[U](callback: T => U): Unit = _traverse(() => false) { item => callback(item) }

  def get(_id: URID): Option[T] = toItem(_id, readBlock(_id))

  def getBatch(offset: URID, numberOfBlocks: Int): Seq[T] = {
    readBlocks(offset, numberOfBlocks) flatMap { case (_id, buf) => toItem(_id, buf)}
  }

  def getRow(_id: URID): RowWithMetadata = {
    val buf = readBlock(_id)
    val rmd = buf.getRowMetaData
    RowWithMetadata(_id, rmd, fields = toFields(buf))
  }

  def getRowMetaData(_id: URID): RowMetaData = RowMetaData.decode(readByte(_id))

  def indexOf(elem: T, fromPos: URID = 0): Int = indexOfOpt(elem, fromPos).getOrElse(-1)

  def indexOfOpt(elem: T, fromPos: URID = 0): Option[URID] = {
    var index_? : Option[URID] = None
    _indexOf(() => index_?.nonEmpty, fromPos) { (_id, item) => if(item == elem) index_? = Option(_id) }
    index_?
  }

  def indexWhere(predicate: T => Boolean): Int = indexWhereOpt(predicate).getOrElse(-1)

  def indexWhereOpt(predicate: T => Boolean): Option[URID] = {
    var index_? : Option[URID] = None
    _indexOf(() => index_?.nonEmpty) { (_id, item) => if (predicate(item)) index_? = Option(_id) }
    index_?
  }

  override def lastOption: Option[T] = if (nonEmpty) get(_id = length - 1) else None

  /**
   * @return the number of records in the file, including the deleted ones.
   *         [[count]] is probably the method you really want.
   */
  def length: URID

  def loadTextFile(file: File)(f: String => Option[T]): PersistentSeq[T] = Source.fromFile(file) use { in =>
    val items = for {line <- in.getLines(); item <- f(line)} yield item
    append(items.toSeq)
  }

  def map[U <: Product : ClassTag](predicate: T => U): PersistentSeq[U] = {
    val that = newDocument[U]()
    _traverse(() => false) { item => that += predicate(item) }
    that
  }

  /**
   * Computes the maximum value of a column
   * @param predicate the search function
   * @return the [[Double maximum value]]
   */
  def max(predicate: T => Double): Double = {
    var maxValue: Double = Double.MinValue
    _traverse(() => false) { item => maxValue = Math.max(maxValue, predicate(item)) }
    maxValue
  }

  /**
   * Computes the minimum value of a column
   * @param predicate the search function
   * @return the [[Double minimum value]]
   */
  def min(predicate: T => Double): Double = {
    var minValue: Double = Double.MaxValue
    _traverse(() => false) { item => minValue = Math.min(minValue, predicate(item)) }
    minValue
  }

  protected def newDocument[A <: Product : ClassTag](): PersistentSeq[A] = PersistentSeq[A]()

  /**
   * Computes the percentile of a column
   * @param predicate the search function
   * @return the [[Double percentile]]
   */
  def percentile(p: Double)(predicate: T => Double): Double = {
    var sample: List[Double] = Nil
    _traverse(() => false) { item => sample = predicate(item) :: sample }
    val index = Math.round(sample.length * (1.0 - p)).toInt
    sample.sorted.apply(index)
  }

  def pop: Option[T] = {
    val p0 = length - 1
    if (p0 < 0) None else {
      val rmd = getRowMetaData(p0)
      if (rmd.isDeleted) None else {
        val item = get(p0)
        shrinkTo(p0)
        item
      }
    }
  }

  def push(item: T): PersistentSeq[T] = append(item)

  def readBlock(offset: URID): ByteBuffer

  def readBlocks(offset: URID, numberOfBlocks: Int = 1): Seq[(URID, ByteBuffer)] = {
    for {
      _id <- offset to offset + numberOfBlocks
    } yield _id -> readBlock(_id)
  }

  def readByte(offset: URID): Byte

  def readBytes(offset: URID, numberOfBlocks: Int = 1): Array[Byte]

  /**
   * Remove an item from the collection via its record offset
   * @param _id the record offset
   */
  def remove(_id: URID): PersistentSeq[T] = setRowMetaData(_id, RowMetaData(isActive = false))

  /**
   * Remove an item from the collection via its record offset
   * @param predicate the search predicate
   * @return the number of records deleted
   */
  def remove(predicate: T => Boolean): Int = {
    var deleted = 0
    _indexOf(() => false) {
      case (_id, item) if predicate(item) => remove(_id); deleted += 1
      case _ =>
    }
    deleted
  }

  def reverse: PersistentSeq[T] = {
    val that = newDocument[T]()
    var (top, bottom) = (0, length - 1)
    while (bottom >= 0) {
      that.writeBytes(top, readBytes(bottom))
      bottom -= 1
      top += 1
    }
    that
  }

  def reverseInPlace(): PersistentSeq[T] = {
    var (top, bottom) = (0, length - 1)
    while (bottom >= 0) {
      if (top != bottom) swap(top, bottom)
      bottom -= 1
      top += 1
    }
    this
  }

  def setRowMetaData(_id: URID, metaData: RowMetaData): PersistentSeq[T] = writeByte(_id, metaData.encode)

  def shrinkTo(newSize: URID): PersistentSeq[T]

  override def slice(start: URID, end: URID): PersistentSeq[T] = {
    val that = newDocument[T]()
    this.copyTo(that, start, end)
    that
  }

  def sortBy[B <: Comparable[B]](predicate: T => B): PersistentSeq[T] = {
    val that = newDocument[T]()
    that ++= toArray.quickSort(predicate)
    that
  }

  /**
   * Computes the sum of a column
   * @param predicate the search function
   * @return the [[Double sum]]
   */
  def sum(predicate: T => Double): Double = {
    var total: Double = 0
    _traverse(() => false) { item => total += predicate(item) }
    total
  }

  def swap(offset0: URID, offset1: URID): Unit = {
    val (block0, block1) = (readBytes(offset0), readBytes(offset1))
    writeBytes(offset0, block1)
    writeBytes(offset1, block0)
  }

  override def tail: PersistentSeq[T] = {
    val that = newDocument[T]()
    copyTo(that, fromPos = 1, toPos = length)
    that
  }

  def take(start: URID, end: URID): Seq[T] = {
    val blockCount = 1 + (end - start).toURID
    val block = readBytes(start, numberOfBlocks = blockCount)

    // transform the block into records
    for {
      index <- 0 until blockCount
      offset = index * recordSize
      item <- toItem(id = start + index, buf = wrap(block, offset, recordSize))
    } yield item
  }

  /**
   * Trims dead entries from of the collection
   * @return the new size of the file
   */
  def trim(): URID = {
    var offset = length - 1
    while (offset >= 0 && getRowMetaData(offset).isDeleted) offset -= 1
    val newLength = offset + 1
    shrinkTo(newLength)
    newLength
  }

  def toArray: Array[T] = {
    val eof = length
    var n: URID = 0
    var m: Int = 0
    val array: Array[T] = new Array[T](count())
    while (n < eof) {
      get(n).foreach { item =>
        array(m) = item
        m += 1
      }
      n += 1
    }
    array
  }

  override def toIterator: Iterator[T] = new PersistenceIterator[T](this)

  override def toTraversable: Traversable[T] = new Traversable[T] {
    override def foreach[U](f: T => U): Unit = PersistentSeq.this.foreach(f)
  }

  def update(_id: URID, item: T): PersistentSeq[T] = writeBytes(_id, toBytes(item))

  def writeBlock(offset: URID, buf: ByteBuffer): PersistentSeq[T] = writeBytes(offset, buf.array())

  def writeBlocks(blocks: Seq[(URID, ByteBuffer)]): PersistentSeq[T]

  def writeBytes(offset: URID, bytes: Array[Byte]): PersistentSeq[T]

  def writeByte(offset: URID, byte: Int): PersistentSeq[T]

  def zip[U <: Product : ClassTag, S <: Product : ClassTag](ps: PersistentSeq[S])(f: (T, S) => U): PersistentSeq[U] = {
    val that = newDocument[U]()
    val eof = Math.min(this.length, ps.length)
    var _id: URID = 0
    while (_id < eof) {
      val item_? = for {itemA <- this.get(_id); itemB <- ps.get(_id)} yield f(itemA, itemB)
      item_?.foreach(that += _)
      _id += 1
    }
    that
  }

  def zipWithIndex[V <: Product : ClassTag, U <: Product : ClassTag](f: (T, URID) => V): PersistentSeq[V] = {
    val that = newDocument[V]()
    val eof = length
    var offset: URID = 0
    while (offset < eof) {
      val item_? = for {item <- get(offset)} yield f(item, offset)
      item_?.foreach(that += _)
      offset += 1
    }
    that
  }

  ///////////////////////////////////////////////////////////////
  //    Private Methods
  ///////////////////////////////////////////////////////////////

  private def _indexOf[U](isDone: () => Boolean, fromPos: URID = 0, toPos: URID = length)(f: (URID, T) => U): PersistentSeq[T] = {
    val batchSize = 1
    var offset = fromPos
    while (offset < toPos && !isDone()) {
      //getBatch(offset, numberOfBlocks = batchSize).zipWithIndex.foreach { case (item, index) => f(offset + index, item) }
      get(offset).foreach { item => f(offset, item) }
      offset += batchSize
    }
    this
  }

  private def _traverse[U](isDone: () => Boolean, fromPos: URID = 0, toPos: URID = length)(f: T => U): PersistentSeq[T] = {
    val batchSize = 1
    var offset = fromPos
    while (offset < toPos && !isDone()) {
      //getBatch(offset, numberOfBlocks = batchSize).foreach(f)
      get(offset).foreach(f)
      offset += batchSize
    }
    this
  }

}

/**
 * PersistentSeq Companion
 */
object PersistentSeq {

  /**
   * Creates a new disk-based sequence implementation
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def apply[A <: Product : ClassTag](): PersistentSeq[A] = new DiskMappedSeq()

  /**
   * Creates a new disk-based sequence implementation
   * @param file the persistence [[File file]]
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def apply[A <: Product : ClassTag](file: File): PersistentSeq[A] = new DiskMappedSeq(file)

  /**
   * Creates a new mixed memory-and-disk sequence implementation
   * @param capacity the collection's storage capacity
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def apply[A <: Product : ClassTag](capacity: Int): PersistentSeq[A] = new HybridPersistentSeq[A](capacity)

  case class RowWithMetadata(_id: URID, metadata: RowMetaData, fields: Seq[Field])

  case class Field(name: String, metadata: FieldMetaData, value: Option[Any])

  /**
   * Persistence Iterator
   * @param device the [[PersistentSeq]]
   * @tparam T the product type
   */
  class PersistenceIterator[T <: Product : ClassTag](device: PersistentSeq[T]) extends Iterator[T] {
    private var item_? : Option[T] = None
    private var pos: URID = 0

    override def hasNext: Boolean = {
      item_? = seekNext()
      item_?.nonEmpty
    }

    override def next: T = item_? match {
      case Some(item) => item_? = None; item
      case None =>
        throw new IllegalStateException("Iterator is empty")
    }

    private def seekNext(): Option[T] = {
      if (pos >= device.length) None else {
        var item_? : Option[T] = None
        do {
          item_? = device.get(pos)
          pos += 1
        } while (pos < device.length && item_?.isEmpty)
        item_?
      }
    }

  }

}