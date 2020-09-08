package com.qwery.database

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import com.qwery.database.DiskMappedSeq.newTempFile
import com.qwery.database.ItemConversion._
import com.qwery.database.PersistentSeq.Row
import com.qwery.util.ResourceHelper._

import scala.concurrent.ExecutionContext
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

  def collect[U](predicate: PartialFunction[T, U]): Stream[U] = iterator.toStream.collect(predicate)

  override def collectFirst[U](predicate: PartialFunction[T, U]): Option[U] = {
    var item_? : Option[U] = None
    _traverse(() => item_?.nonEmpty) { item => item_? = Option(predicate(item)) }
    item_?
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

  override def exists(predicate: T => Boolean): Boolean = {
    var isFound = false
    _traverse(() => isFound) { item => if (predicate(item)) isFound = true }
    isFound
  }

  override def filter(predicate: T => Boolean): PersistentSeq[T] = {
    val that = newDocument[T]()
    _gather() { item => if (predicate(item)) that += item }
    that
  }

  override def filterNot(predicate: T => Boolean): PersistentSeq[T] = {
    val that = newDocument[T]()
    _gather() { item => if (!predicate(item)) that += item }
    that
  }

  def firstIndexOption: Option[URID] = {
    var offset = 0
    while (offset < length && getRowMetaData(offset).isDeleted) offset += 1
    if (offset < length) Some(offset) else None
  }

  def flatMap[U <: Product : ClassTag](predicate: T => TraversableOnce[U]): Stream[U] = iterator.toStream.flatMap(predicate)

 override def foreach[U](callback: T => U): Unit = _gather() { item => callback(item) }

  def get(_id: URID): Option[T] = toItem(_id, readBlock(_id))

  def getBatch(offset: URID, numberOfItems: Int): Seq[T] = {
    readBlocks(offset, numberOfItems) flatMap { case (_id, buf) => toItem(_id, buf) }
  }

  def getRow(_id: URID): Row = {
    val buf = readBlock(_id)
    val rmd = buf.getRowMetaData
    Row(_id, rmd, fields = toFields(buf))
  }

  def getRowMetaData(_id: URID): RowMetaData = RowMetaData.decode(readByte(_id))

  override def headOption: Option[T] = firstIndexOption.flatMap(get)

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

  protected def intoBlocks(offset: URID, src: Array[Byte]): Seq[(URID, ByteBuffer)] = {
    val count = src.length / recordSize
    for (index <- 0 to count) yield {
      val buf = new Array[Byte](recordSize)
      System.arraycopy(src, index * recordSize, buf, 0, Math.min(buf.length, src.length - index * recordSize))
      (offset + index) -> wrap(buf)
    }
  }

  def iterator: Iterator[T] = new Iterator[T] {
    private var item_? : Option[T] = None
    private var offset: URID = 0
    private val eof = PersistentSeq.this.length -1

    override def hasNext: Boolean = {
      offset = _findNext(fromPos = offset)(_.isActive).getOrElse(eof)
      item_? = if(offset < eof) get(offset) else None
      offset += 1
      item_?.nonEmpty
    }

    override def next: T = item_? match {
      case Some(item) => item_? = None; item
      case None =>
        throw new IllegalStateException("Iterator is empty")
    }
  }

  def lastIndexOption: Option[URID] = {
    var offset = length - 1
    while (offset > 0 && getRowMetaData(offset).isDeleted) offset -= 1
    if (offset > 0) Some(offset) else None
  }

  override def lastOption: Option[T] = lastIndexOption.flatMap(get)

  /**
   * @return the number of records in the file, including the deleted ones.
   *         [[count]] is probably the method you really want.
   */
  def length: URID

  def loadTextFile(file: File)(f: String => Option[T]): PersistentSeq[T] = Source.fromFile(file) use { in =>
    val items = for {line <- in.getLines(); item <- f(line)} yield item
    append(items.toSeq)
  }

  def map[U <: Product : ClassTag](predicate: T => U): Stream[U] = iterator.toStream.map(predicate)

  /**
   * Computes the maximum value of a column
   * @param predicate the search function
   * @return the [[Double maximum value]]
   */
  def max(predicate: T => Double): Double = {
    var maxValue: Double = Double.MinValue
    _gather() { item => maxValue = Math.max(maxValue, predicate(item)) }
    maxValue
  }

  /**
   * Computes the minimum value of a column
   * @param predicate the search function
   * @return the [[Double minimum value]]
   */
  def min(predicate: T => Double): Double = {
    var minValue: Double = Double.MaxValue
    _gather() { item => minValue = Math.min(minValue, predicate(item)) }
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
    _gather() { item => sample = predicate(item) :: sample }
    val index = Math.round(sample.length * (1.0 - p)).toInt
    sample.sorted.apply(index)
  }

  def pop: Option[T] = lastIndexOption flatMap { offset =>
    val item = get(offset)
    remove(offset)
    item
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
      that.writeBlock(top, readBlock(bottom))
      bottom -= 1
      top += 1
    }
    that
  }

  def reverseIterator: Iterator[T] = _reverseIterator.flatMap(t => toItem(t._1, t._2))

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
   * @param predicate the search predicate
   * @return the [[Double sum]]
   */
  def sum(predicate: T => Double): Double = {
    var total: Double = 0
    _gather() { item => total += predicate(item) }
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

  override def toTraversable: Traversable[T] = this

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
  //    Utility Methods
  ///////////////////////////////////////////////////////////////

  private def _findNext(fromPos: URID = 0, forward: Boolean = true)(f: RowMetaData => Boolean): Option[URID] = {
    var offset = fromPos
    if (forward) {
      while (offset < length && !f(getRowMetaData(offset))) offset += 1
      if (offset < length) Some(offset) else None
    }
    else {
      while (offset >= 0 && !f(getRowMetaData(offset))) offset -= 1
      if (offset >= 0) Some(offset) else None
    }
  }

  private def _gather[U](fromPos: URID = 0, toPos: URID = length)(f: T => U): PersistentSeq[T] = {
    val batchSize = 1
    var offset = fromPos
    while (offset < toPos) {
      //getBatch(offset, numberOfBlocks = batchSize).foreach(f)
      get(offset).foreach(f)
      offset += batchSize
    }
    this
  }

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

  private def _reverseIterator: Iterator[(URID, ByteBuffer)] = new Iterator[(URID, ByteBuffer)] {
    private var item_? : Option[(URID, ByteBuffer)] = None
    private var offset: URID = PersistentSeq.this.length - 1

    override def hasNext: Boolean = {
      offset = _findNext(fromPos = offset, forward = false)(_.isActive).getOrElse(-1)
      item_? = if(offset > -1) Some(offset -> readBlock(offset)) else None
      offset -= 1
      item_?.nonEmpty
    }

    override def next: (URID, ByteBuffer) = item_? match {
      case Some(item) => item_? = None; item
      case None =>
        throw new IllegalStateException("Iterator is empty")
    }
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
   * Creates a new persistent sequence implementation
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def apply[A <: Product : ClassTag](): PersistentSeq[A] = new DiskMappedSeq()

  def builder[A <: Product : ClassTag]: Builder[A] = new PersistentSeq.Builder[A]()

  /**
   * Creates a new disk-based sequence implementation
   * @param file the persistence [[File file]]
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def disk[A <: Product : ClassTag](file: File = newTempFile()): PersistentSeq[A] = new DiskMappedSeq[A](file)

  /**
   * Creates a new mixed memory and disk-mapped sequence implementation
   * @param capacity the collection's storage capacity
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def hybrid[A <: Product : ClassTag](capacity: Int): PersistentSeq[A] = new HybridPersistentSeq[A](capacity)

  /**
   * Creates a new memory-mapped sequence implementation
   * @param capacity the collection's storage capacity
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def memory[A <: Product : ClassTag](capacity: Int): PersistentSeq[A] = new MemoryMappedSeq[A](capacity)

  /**
   * Creates a new high-performance partitioned sequence implementation
   * @param partitionSize the partition size (e.g. maximum number of items)
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def parallel[A <: Product : ClassTag](partitionSize: Int)(implicit ec: ExecutionContext) = new ParallelPersistentSeq[A](partitionSize)

  /**
   * Creates a new partitioned sequence implementation
   * @param partitionSize the partition size (e.g. maximum number of items)
   * @tparam A the product class
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def partitioned[A <: Product : ClassTag](partitionSize: Int) = new PartitionedPersistentSeq[A](partitionSize)

  /**
   * PersistentSeq Builder
   * @tparam A the product type
   */
  class Builder[A <: Product : ClassTag]() {
    private var capacity: Int = 0
    private var executionContext: ExecutionContext = _
    private var partitionSize: Int = 0
    private var persistenceFile: File = _

    def build: PersistentSeq[A] = {
      // is it in memory?
      if (capacity > 0) {
        if (persistenceFile != null) hybrid(capacity) else memory(capacity)
      }

      // is it partitioned?
      else if (partitionSize > 0) {
        if (executionContext != null) {
          implicit val ec: ExecutionContext = executionContext
          parallel[A](partitionSize)
        }
        else partitioned[A](partitionSize)
      }

      // just use ole faithful
      else disk(Option(persistenceFile).getOrElse(newTempFile()))
    }

    def withMemoryCapacity(capacity: Int): this.type = {
      this.capacity = capacity
      this
    }

    def withParallelism(executionContext: ExecutionContext): this.type = {
      this.executionContext = executionContext
      this
    }

    def withPartitions(partitionSize: Int): this.type = {
      this.partitionSize = partitionSize
      this
    }

    def withPersistenceFile(file: File): this.type = {
      this.persistenceFile = file
      this
    }
  }

  case class Field(name: String, metadata: FieldMetaData, value: Option[Any])

  case class Row(_id: URID, metadata: RowMetaData, fields: Seq[Field])

  /**
   * Persistence Iterator
   * @param device the [[PersistentSeq]]
   * @tparam T the product type
   */
  class PersistenceForwardIterator[T <: Product : ClassTag](device: PersistentSeq[T]) extends Iterator[T] {
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