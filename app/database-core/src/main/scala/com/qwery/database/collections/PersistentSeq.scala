package com.qwery.database
package collections

import com.qwery.database.device._
import com.qwery.database.files.DatabaseFiles
import com.qwery.database.models.{BinaryRow, Field, Row, RowMetadata}
import com.qwery.database.util.Codec._
import com.qwery.models.EntityRef
import com.qwery.util.ResourceHelper._

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.collection.{GenIterable, mutable}
import scala.io.Source
import scala.language.postfixOps
import scala.reflect.ClassTag

/**
 * Represents a persistent sequential collection
 * @param device  the [[BlockDevice block device]]
 * @param `class` the [[Class product class]]
 * @tparam T the [[Product product]] type
 */
class PersistentSeq[T <: Product](val device: BlockDevice, `class`: Class[T]) extends Traversable[T] {
  // cache the class information for type T
  private val declaredFields = `class`.getDeclaredFields.toList
  private val declaredFieldNames = declaredFields.map(_.getName)
  private val constructor = `class`.getConstructors.find(_.getParameterCount == declaredFields.length)
    .getOrElse(die(s"No suitable constructor found for class ${`class`.getName}"))
  private val parameterTypes = constructor.getParameterTypes

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
    device.writeRowAsBinary(device.length, toBytes(item))
    this
  }

  /**
   * Appends the collection of items to the end of the file
   * @param items the collection of [[T item]] to append
   * @return [[PersistentSeq self]]
   */
  def append(items: Traversable[T]): PersistentSeq[T] = {
    device.writeRows(toBinaryRows(device.length, items))
    this
  }

  /**
   * Retrieves the item corresponding to the record offset
   * @param rowID the record offset
   * @return the [[T item]]
   */
  def apply(rowID: ROWID): T = {
    toItem(row = device.readRow(rowID), evenDeletes = true).getOrElse(die("No record found"))
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

  override def copyToArray[B >: T](array: Array[B], start: Int, len: Int): Unit = {
    var n: Int = 0
    for {
      rowID <- start until (start + len)
      row = device.readRow(rowID)
      item <- toItem(row, evenDeletes = false)
    } {
      array(n) = item
      n += 1
    }
  }

  /**
   * Counts all active rows
   * @return the number of active rows
   */
  def count(): Long = device.countRows(_.isActive)

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
   * Creates an item from a collection of fields
   * @param items the collection of [[Field fields]]
   * @return a new [[T item]]
   */
  def createItem(items: Seq[Field]): T = {
    val nameToValueMap: Map[String, Option[Any]] = Map(items.collect { case f if f.value.nonEmpty => f.name -> f.value }: _*)
    val rawValues = declaredFieldNames.flatMap(nameToValueMap.get)
    val normalizedValues = (parameterTypes zip rawValues) map { case (param, value) =>
      if (param == classOf[Option[_]]) value else value.map(_.asInstanceOf[AnyRef]).orNull
    }
    constructor.newInstance(normalizedValues: _*).asInstanceOf[T]
  }

  override def exists(predicate: T => Boolean): Boolean = {
    var isFound = false
    _traverse(() => isFound) { item => if (predicate(item)) isFound = true }
    isFound
  }

  override def filter(predicate: T => Boolean): Stream[T] = toStream.filter(predicate)

  override def filterNot(predicate: T => Boolean): Stream[T] = toStream.filterNot(predicate)

  def flatMap[U](predicate: T => TraversableOnce[U]): Stream[U] = toStream.flatMap(predicate)

  override def foreach[U](callback: T => U): Unit = {
    device.foreachBinary { row => toItem(row, evenDeletes = false).foreach(callback) }
  }

  def get(rowID: ROWID): Option[T] = toItem(row = device.readRow(rowID), evenDeletes = false)

  override def headOption: Option[T] = device.firstIndexOption.flatMap(get)

  def indexOf(elem: T, fromPos: ROWID = 0): ROWID = indexOfOpt(elem, fromPos).getOrElse(-1L: ROWID)

  def indexOfOpt(elem: T, fromPos: ROWID = 0): Option[ROWID] = {
    var index_? : Option[ROWID] = None
    _indexOf(() => index_?.nonEmpty, fromPos) { (rowID, item) => if(item == elem) index_? = Option(rowID) }
    index_?
  }

  def indexWhere(predicate: T => Boolean): ROWID = indexWhereOpt(predicate).getOrElse(-1L: ROWID)

  def indexWhereOpt(predicate: T => Boolean): Option[ROWID] = {
    var index_? : Option[ROWID] = None
    _indexOf(() => index_?.nonEmpty) { (rowID, item) => if (predicate(item)) index_? = Option(rowID) }
    index_?
  }

  def iterator: Iterator[T] = new Iterator[T] {
    private var item_? : Option[T] = None
    private var rowID: ROWID = 0
    private val eof = device.length

    override def hasNext: Boolean = {
      rowID = device.findRow(fromPos = rowID)(_.isActive).getOrElse(eof)
      item_? = if (rowID < eof) get(rowID) else None
      rowID += 1
      item_?.nonEmpty
    }

    override def next: T = item_? match {
      case Some(item) => item_? = None; item
      case None => die("Iterator is empty")
    }
  }

  override def lastOption: Option[T] = device.lastIndexOption.flatMap(get)

  def length: ROWID = device.length

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
    _gather() { item => minValue = minValue min predicate(item) }
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

  def pop(): Option[T] = device.lastIndexOption flatMap { rowID =>
    val item = get(rowID)
    device.remove(rowID)
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
      case (rowID, item) if predicate(item) => device.remove(rowID); deleted += 1
      case _ =>
    }
    deleted
  }

  def reverse: Stream[T] = reverseIterator.toStream

  def reverseIterator: Iterator[T] = device.reverseIterator.flatMap(t => toItem(t._1, t._2, evenDeletes = false))

  override def slice(start: Int, end: Int): Stream[T] = {
    device.readRows(start, numberOfRows = 1 + (end - start)) flatMap { row => toItem(row, evenDeletes = false) } toStream
  }

  /**
   * Performs an in-memory sorting of the collection
   * @param predicate the sort predicate
   */
  def sortBy[B <: Comparable[B]](predicate: T => B): Stream[T] = toStream.sortBy(predicate)

  /**
   * Performs an in-place sorting of the collection
   * @param predicate the sort predicate
   */
  def sortInPlace[B <: Comparable[B]](predicate: T => B): Unit = {
    val cache = mutable.Map[ROWID, Option[B]]()

    def fetch(rowID: ROWID): Option[B] = cache.getOrElseUpdate(rowID, get(rowID).map(predicate))

    def partition(low: ROWID, high: ROWID): ROWID = {
      var m = low - 1 // index of lesser item
      for {
        pivot <- fetch(high)
        n <- low until high
        value <- fetch(n) if value.compareTo(pivot) < 0
      } {
        m += 1 // increment the index of lesser item
        swap(m, n)
      }
      swap(m + 1, high)
      m + 1
    }

    def sort(low: ROWID, high: ROWID): Unit = if (low < high) {
      val pi = partition(low, high)
      sort(low, pi - 1)
      sort(pi + 1, high)
    }

    def swap(offset0: ROWID, offset1: ROWID): Unit = {
      val (elem0, elem1) = (cache.remove(offset0), cache.remove(offset1))
      device.swap(offset0, offset1)
      elem0.foreach(v => cache(offset1) = v)
      elem1.foreach(v => cache(offset0) = v)
    }

    sort(low = 0, high = device.length - 1)
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

  override def toArray[B >: T : ClassTag]: Array[B] = {
    val eof: ROWID = device.length
    var n: ROWID = 0
    var m: Int = 0
    val array: Array[B] = new Array[B](count().toInt)
    while (n < eof) {
      get(n).foreach { item =>
        array(m) = item
        m += 1
      }
      n += 1
    }
    array
  }

  def toBinaryRows(offset: ROWID, items: Traversable[T]): Stream[BinaryRow] = {
    items.toStream.zipWithIndex.map { case (item, index) => BinaryRow(id = offset + index, buf = toBytes(item))(device) }
  }

  def toBlocks(offset: ROWID, items: Traversable[T]): Stream[(ROWID, ByteBuffer)] = {
    items.toStream.zipWithIndex.map { case (item, index) => (offset + index) -> toBytes(item) }
  }

  def toBytes(item: T): ByteBuffer = {
    val payloads = for {
      (name, value_?) <- toKeyValues(item)
      column <- device.nameToColumnMap.get(name).toArray if !column.isLogical
    } yield encode(column, value_?)

    // convert the row to binary
    val buf = allocate(device.recordSize).putRowMetadata(RowMetadata())
    payloads.zipWithIndex foreach { case (bytes, index) =>
      buf.position(device.columnOffsets(index))
      buf.put(bytes)
    }
    buf.flip()
    buf
  }

  def toBytes(items: Seq[T]): Seq[ByteBuffer] = items.map(toBytes)

  def toBytes(items: Traversable[T]): Stream[ByteBuffer] = items.toStream.map(toBytes)

  def toItem(id: ROWID, buf: ByteBuffer, evenDeletes: Boolean): Option[T] = {
    val metadata = buf.getRowMetadata
    if (metadata.isActive || evenDeletes) Some(createItem(items = device.toRowIdField(id).toList ::: Row.toFields(buf)(device).toList)) else None
  }

  def toItem(row: BinaryRow, evenDeletes: Boolean): Option[T] = {
    if (row.metadata.isActive || evenDeletes) Some(createItem(items = device.toRowIdField(row.id).toList ::: row.toFields(device).toList)) else None
  }

  override def toIterator: Iterator[T] = iterator

  def toKeyValues(product: T): Seq[KeyValue] = declaredFieldNames zip product.productIterator.toSeq map {
    case (name, value: Option[_]) => name -> value
    case (name, value) => name -> Option(value)
  }

  override def toTraversable: Traversable[T] = this

  def update(rowID: ROWID, item: T): Unit = device.writeRowAsBinary(rowID, toBytes(item))

  def zip[U](that: GenIterable[U]): Iterator[(T, U)] = this.iterator zip that.iterator

  def zipWithIndex: Iterator[(T, Int)] = this.iterator.zipWithIndex

  ///////////////////////////////////////////////////////////////
  //    Utility Methods
  ///////////////////////////////////////////////////////////////

  private def _gather[U](fromPos: ROWID = 0, toPos: ROWID = device.length)(f: T => U): Unit = {
    var rowID: ROWID = fromPos
    while (rowID < toPos) {
      get(rowID).foreach(f)
      rowID += 1
    }
  }

  private def _indexOf[U](isDone: () => Boolean, fromPos: ROWID = 0, toPos: ROWID = device.length)(f: (ROWID, T) => U): Unit = {
    var rowID: ROWID = fromPos
    while (rowID < toPos && !isDone()) {
      get(rowID).foreach(f(rowID, _))
      rowID += 1
    }
  }

  private def _traverse[U](isDone: () => Boolean, fromPos: ROWID = 0, toPos: ROWID = device.length)(f: T => U): Unit = {
    var rowID: ROWID = fromPos
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
   * Creates a persistent sequence implementation
   * @tparam A the [[Product product type]]
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def apply[A <: Product : ClassTag](): PersistentSeq[A] = apply[A](BlockDevice.builder.withRowModel[A])

  /**
    * Creates a persistent sequence implementation
    * @tparam A the [[Product product type]]
    * @param builder the [[BlockDevice.Builder device builder]]
    * @return a new [[PersistentSeq persistent sequence]]
    */
  def apply[A <: Product : ClassTag](builder: BlockDevice.Builder): PersistentSeq[A] = {
    val (columns, _class) = BlockDevice.toColumns[A]
    val device = builder.withColumns(columns).build
    new PersistentSeq[A](device, _class)
  }

  /**
   * Creates a disk-based sequence implementation
   * @param persistenceFile the persistence [[File file]]
   * @tparam A the [[Product product type]]
   * @return a new [[PersistentSeq persistent sequence]]
   */
  def apply[A <: Product : ClassTag](persistenceFile: File): PersistentSeq[A] = {
    apply[A](BlockDevice.builder.withRowModel[A].withPersistenceFile(persistenceFile))
  }

  /**
    * Creates a table-based sequence implementation
    * @param databaseName the name of the database
    * @param tableName    the name of the table
    * @tparam A the [[Product product type]]
    * @return a new [[PersistentSeq persistent sequence]]
    */
  def apply[A <: Product : ClassTag](ref: EntityRef): PersistentSeq[A] = {
    val (config, device) = DatabaseFiles.getTableDevice(ref)
    val (columns, _class) = BlockDevice.toColumns[A]
    val deviceColumns = config.columns.map(c => c.name -> c.`type`)
    val productColumns = columns.map(c => c.name -> c.`type`)
    val missingColumns = deviceColumns.collect { case t@(name, _type) if !productColumns.contains(t) => name + ':' + _type }
    assert(missingColumns.isEmpty, s"Class ${_class.getName} does not contain columns: ${missingColumns.mkString(", ")}")
    new PersistentSeq[A](device, _class)
  }

}