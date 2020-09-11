package com.qwery.database

import java.io.File

import com.qwery.database.DiskMappedSeq.newTempFile
import com.qwery.util.ResourceHelper._

import scala.collection.{GenIterable, mutable}
import scala.concurrent.ExecutionContext
import scala.io.Source
import scala.language.postfixOps
import scala.reflect.ClassTag

/**
 * Represents a persistent sequential collection
 */
abstract class PersistentSeq[T <: Product : ClassTag] extends BinaryTable[T] with Traversable[T] {

  /**
   * Appends a collection of items to the end of this collection
   * @param items the collection of [[T items]] to append
   */
  def ++=(items: Traversable[T]): Unit = append(items)

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
  def append(item: T): PersistentSeq[T] = {
    writeBlock(length, toBytes(item))
    this
  }

  /**
   * Appends the collection of items to the end of the file
   * @param items the collection of [[T item]] to append
   * @return [[PersistentSeq self]]
   */
  def append(items: Traversable[T]): PersistentSeq[T] = {
    writeBlocks(toBlocks(length, items))
    this
  }

  /**
   * Retrieves the item corresponding to the record offset
   * @param rowID the record offset
   * @return the [[T item]]
   */
  def apply(rowID: ROWID): T = {
    toItem(rowID, buf = readBlock(rowID), evenDeletes = true)
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

  def collect[U](predicate: PartialFunction[T, U]): Stream[U] = iterator.toStream.collect(predicate)

  override def collectFirst[U](predicate: PartialFunction[T, U]): Option[U] = {
    var item_? : Option[U] = None
    _traverse(() => item_?.nonEmpty) { item => item_? = Option(predicate(item)) }
    item_?
  }

  def contains(elem: T): Boolean = indexOfOpt(elem).nonEmpty

  override def copyToArray[B >: T](array: Array[B], start: ROWID, len: ROWID): Unit = {
    var n: Int = 0
    for {
      (rowID, buf) <- readBlocks(start, len)
      item <- toItem(rowID, buf)
    } {
      array(n) = item
      n += 1
    }
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

  override def exists(predicate: T => Boolean): Boolean = {
    var isFound = false
    _traverse(() => isFound) { item => if (predicate(item)) isFound = true }
    isFound
  }

  override def filter(predicate: T => Boolean): Stream[T] = toStream.filter(predicate)

  override def filterNot(predicate: T => Boolean): Stream[T] = toStream.filterNot(predicate)

  def flatMap[U](predicate: T => TraversableOnce[U]): Stream[U] = toStream.flatMap(predicate)

  override def foreach[U](callback: T => U): Unit = _gather() { callback }

  def get(rowID: ROWID): Option[T] = toItem(rowID, readBlock(rowID))

  override def headOption: Option[T] = firstIndexOption.flatMap(get)

  def indexOf(elem: T, fromPos: ROWID = 0): Int = indexOfOpt(elem, fromPos).getOrElse(-1)

  def indexOfOpt(elem: T, fromPos: ROWID = 0): Option[ROWID] = {
    var index_? : Option[ROWID] = None
    _indexOf(() => index_?.nonEmpty, fromPos) { (rowID, item) => if(item == elem) index_? = Option(rowID) }
    index_?
  }

  def indexWhere(predicate: T => Boolean): Int = indexWhereOpt(predicate).getOrElse(-1)

  def indexWhereOpt(predicate: T => Boolean): Option[ROWID] = {
    var index_? : Option[ROWID] = None
    _indexOf(() => index_?.nonEmpty) { (rowID, item) => if (predicate(item)) index_? = Option(rowID) }
    index_?
  }

  def iterator: Iterator[T] = new Iterator[T] {
    private var item_? : Option[T] = None
    private var offset: ROWID = 0
    private val eof = PersistentSeq.this.length

    override def hasNext: Boolean = {
      offset = findRow(fromPos = offset)(_.isActive).getOrElse(eof)
      item_? = if (offset < eof) get(offset) else None
      offset += 1
      item_?.nonEmpty
    }

    override def next: T = item_? match {
      case Some(item) => item_? = None; item
      case None =>
        throw new IllegalStateException("Iterator is empty")
    }
  }

  override def lastOption: Option[T] = lastIndexOption.flatMap(get)

  def loadTextFile(file: File)(f: String => Option[T]): PersistentSeq[T] = Source.fromFile(file) use { in =>
    val items = for {line <- in.getLines(); item <- f(line)} yield item
    append(items.toSeq)
  }

  def map[U](predicate: T => U): Stream[U] = toStream.map(predicate)

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

  def pop: Option[T] = lastIndexOption flatMap { rowID =>
    val item = get(rowID)
    remove(rowID)
    item
  }

  def push(item: T): PersistentSeq[T] = append(item)

  /**
   * Remove an item from the collection via its record offset
   * @param predicate the search predicate
   * @return the number of records deleted
   */
  def remove(predicate: T => Boolean): Int = {
    var deleted = 0
    _indexOf(() => false) {
      case (rowID, item) if predicate(item) => remove(rowID); deleted += 1
      case _ =>
    }
    deleted
  }

  def reverse: Stream[T] = reverseIterator.toStream

  def reverseIterator: Iterator[T] = reverseIteration.flatMap(t => toItem(t._1, t._2))

  override def slice(start: ROWID, end: ROWID): Stream[T] = {
    readBlocks(start, numberOfBlocks = 1 + (end - start)) flatMap { case (rowID, buf) => toItem(rowID, buf) } toStream
  }

  /**
   * Performs an in-memory sorting of the items
   * @param predicate the sort predicate
   */
  def sortBy[B <: Comparable[B]](predicate: T => B): Stream[T] = toStream.sortBy(predicate)

  /**
   * Performs an in-place sorting of the items
   * @param predicate the sort predicate
   */
  def sortByInPlace[B <: Comparable[B]](predicate: T => B): Unit = {
    val cache = mutable.Map[ROWID, Option[B]]()

    def fetch(rowID: ROWID): Option[B] = cache.getOrElseUpdate(rowID, get(rowID).map(predicate))

    def partition(low: ROWID, high: ROWID): ROWID = {
      var i = low - 1 // index of lesser item
      for {
        pivot <- fetch(high)
        j <- low until high
        value <- fetch(j)
      } {
        if (value.compareTo(pivot) < 0) {
          i += 1 // increment the index of lesser item
          swap(i, j)
        }
      }
      swap(i + 1, high)
      i + 1
    }

    def sort(low: ROWID, high: ROWID): Unit = if (low < high) {
      val pi = partition(low, high)
      sort(low, pi - 1)
      sort(pi + 1, high)
    }

    def swap(offset0: ROWID, offset1: ROWID): Unit = {
      val (elem0, elem1) = (cache.remove(offset0), cache.remove(offset1))
      PersistentSeq.this.swap(offset0, offset1)
      elem0.foreach(v => cache(offset1) = v)
      elem1.foreach(v => cache(offset0) = v)
    }

    sort(low = 0, high = length - 1)
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

  override def tail: Stream[T] = toStream.tail

  def toArray: Array[T] = {
    val eof = length
    var n: ROWID = 0
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

  override def toIterator: Iterator[T] = iterator

  override def toTraversable: Traversable[T] = this

  def update(rowID: ROWID, item: T): Unit = writeBlock(rowID, toBytes(item))

  def zip[U](that: GenIterable[U]): Iterator[(T, U)] = this.iterator zip that.iterator

  def zipWithIndex: Iterator[(T, Int)] = this.iterator.zipWithIndex

  ///////////////////////////////////////////////////////////////
  //    Utility Methods
  ///////////////////////////////////////////////////////////////

  private def _gather[U](fromPos: ROWID = 0, toPos: ROWID = length)(f: T => U): Unit = {
    var rowID = fromPos
    while (rowID < toPos) {
      get(rowID).foreach(f)
      rowID += 1
    }
  }

  private def _indexOf[U](isDone: () => Boolean, fromPos: ROWID = 0, toPos: ROWID = length)(f: (ROWID, T) => U): Unit = {
    var rowID = fromPos
    while (rowID < toPos && !isDone()) {
      get(rowID).foreach { item => f(rowID, item) }
      rowID += 1
    }
  }

  private def _traverse[U](isDone: () => Boolean, fromPos: ROWID = 0, toPos: ROWID = length)(f: T => U): Unit = {
    var rowID = fromPos
    while (rowID < toPos && !isDone()) {
      get(rowID).foreach(f)
      rowID += 1
    }
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

  case class Row(rowID: ROWID, metadata: RowMetaData, fields: Seq[Field])

  /**
   * Persistence Iterator
   * @param device the [[PersistentSeq]]
   * @tparam T the product type
   */
  class PersistenceForwardIterator[T <: Product : ClassTag](device: PersistentSeq[T]) extends Iterator[T] {
    private var item_? : Option[T] = None
    private var pos: ROWID = 0

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