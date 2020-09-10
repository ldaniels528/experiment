package com.qwery.database

import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.ClassTag

/**
 * Represents a multi-threaded partitioned persistent sequence
 * @param partitionSize the maximum size of each partition
 * @tparam T the product type
 */
class ParallelPersistentSeq[T <: Product : ClassTag](partitionSize: Int)(implicit ec: ExecutionContext)
  extends PartitionedPersistentSeq[T](partitionSize) {

 override def avg(predicate: T => Double): Double = {
   val values = Await.result(Future.sequence(partitions map { partition =>
     Future(partition.avg(predicate))
   }), Duration.Inf)
   if (values.nonEmpty) values.sum / values.length else Double.NaN
  }

  override def count(): Int = countRows(_.isActive)

  override def count(predicate: T => Boolean): Int = {
    Await.result(Future.sequence(partitions map { partition =>
      Future(partition.count(predicate))
    }).map(_.sum), Duration.Inf)
  }

  override def countRows(predicate: RowMetaData => Boolean): Int = {
    Await.result(Future.sequence(partitions map { partition =>
      Future(partition.countRows(predicate))
    }).map(_.sum), Duration.Inf)
  }

  override def filter(predicate: T => Boolean): PersistentSeq[T] = {
    combine(Await.result(Future.sequence(partitions map { partition =>
      Future(partition.filter(predicate))
    }), Duration.Inf))
  }

  override def filterNot(predicate: T => Boolean): PersistentSeq[T] = {
    combine(files = Await.result(Future.sequence(partitions map { partition =>
      Future(partition.filterNot(predicate))
    }), Duration.Inf))
  }

  override def forall(predicate: T => Boolean): Boolean = {
    val values = Await.result(Future.sequence(partitions map { partition =>
      Future(partition.forall(predicate))
    }), Duration.Inf)
    values.forall(_ => true)
  }

  override def toIterator: Iterator[T] = new Iterator[T] {
    private val iterators = partitions.map(_.toIterator)
    private var index = 0

    @tailrec
    override def hasNext: Boolean = {
      val isActive = iterators(index).hasNext
      if (isActive) isActive
      else if (index < iterators.length) {
        index += 1
        hasNext
      }
      else false
    }

    override def next(): T = iterators(index).next()
  }

  override def min(predicate: T => Double): Double = {
    val values = Await.result(Future.sequence(partitions map { partition =>
      Future(partition.min(predicate))
    }), Duration.Inf)
    if(values.nonEmpty) values.min else Double.NaN
  }

  override def max(predicate: T => Double): Double = {
    val values = Await.result(Future.sequence(partitions map { partition =>
      Future(partition.max(predicate))
    }), Duration.Inf)
    if(values.nonEmpty) values.max else Double.NaN
  }

  override def readBlocks(offset: ROWID, numberOfBlocks: ROWID): Seq[(ROWID, ByteBuffer)] = {
    // determine the partitions we need to read from
    val mappings = getReadPartitionMappings(offset, numberOfBlocks)

    // asynchronously read the blocks
    val outcome = Future.sequence(mappings map { case (partition, offsets) =>
      Future(partition.readBlocks(offsets.head, numberOfBlocks = offsets.size))
    }) map (_.flatten)
    Await.result(outcome, Duration.Inf)
  }

  override def readBytes(offset: ROWID, numberOfBlocks: ROWID): Array[Byte] = {
    // determine the partitions we need to read from
    val mappings = getReadPartitionMappings(offset, numberOfBlocks)

    // asynchronously read the bytes
    val outcome = Future.sequence(mappings map { case (partition, offsets) =>
      Future(partition.readBytes(offsets.head, numberOfBlocks = offsets.size))
    }) map (_.toArray.flatten)
    Await.result(outcome, Duration.Inf)
  }

  override def remove(predicate: T => Boolean): Int = {
    Await.result(Future.sequence(partitions map { partition =>
      Future(partition.remove(predicate))
    }).map(_.sum), Duration.Inf)
  }

  override def reverse: PersistentSeq[T] = {
    combine(files = Await.result(Future.sequence(partitions map { partition =>
      Future(partition.reverse)
    }), Duration.Inf))
  }

  override def reverseInPlace(): PersistentSeq[T] = {
    combine(files = Await.result(Future.sequence(partitions map { partition =>
      Future(partition.reverseInPlace())
    }), Duration.Inf))
  }

  override def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): PersistentSeq[T] = {
    case class Datum(partition: PersistentSeq[T], offset: ROWID, buf: ByteBuffer)

    // determine the partitions we need to write to
    val results =
      (for {
        (globalOffset, buf) <- blocks
        index = toPartitionIndex(globalOffset)
        partition = partitions(index)
        localOffset = toLocalOffset(globalOffset, index)
      } yield Datum(partition, localOffset, buf)).groupBy(_.partition).toSeq

    // asynchronously write the blocks
    val outcome = Future.sequence(results map { case (partition, datum) =>
      Future(partition.writeBlocks(datum.map { case Datum(_, offset, buf) => (offset, buf) }))
    })
    Await.ready(outcome, Duration.Inf)
    this
  }

  override def writeBytes(offset: ROWID, bytes: Array[Byte]): PersistentSeq[T] = writeBlocks(intoBlocks(offset, bytes))

  private def combine(files: Seq[PersistentSeq[T]]): PartitionedPersistentSeq[T] = {
    val that = new PartitionedPersistentSeq[T](partitionSize)
    files.filter(_.nonEmpty).foreach(that.addPartition)
    that
  }

  private def getReadPartitionMappings(offset: ROWID, numberOfBlocks: ROWID): Seq[(PersistentSeq[T], Seq[ROWID])] = {
    // determine the partitions we need to read from
    case class PartitionAndOffset(partition: PersistentSeq[T], offset: ROWID)

    (for {
      globalOffset <- offset to offset + numberOfBlocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      localOffset = toLocalOffset(globalOffset, index)
    } yield PartitionAndOffset(partition, localOffset))
      .groupBy(_.partition)
      .map { case (partition, offsets) => partition -> offsets.map(_.offset) }
      .toSeq
  }

}
