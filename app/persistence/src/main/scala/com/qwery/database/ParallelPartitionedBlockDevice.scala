package com.qwery.database

import java.io.File
import java.nio.ByteBuffer

import com.qwery.database.BlockDevice.RowStatistics
import com.qwery.database.PersistentSeq.newTempFile

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Parallel Partitioned Block Device
 * @param columns       the collection of [[Column columns]]
 * @param partitionSize the size of each partition
 * @param ec            the implicit [[ExecutionContext]]
 */
class ParallelPartitionedBlockDevice(columns: Seq[Column],
                                     partitionSize: Int,
                                     persistenceDirectory: File = newTempFile().getParentFile,
                                     isInMemory: Boolean = false)(implicit ec: ExecutionContext)
  extends PartitionedBlockDevice(columns, partitionSize, persistenceDirectory, isInMemory) {

  override def countRows(predicate: RowMetadata => Boolean): ROWID = {
    Await.result(Future.sequence(partitions map { partition =>
      Future(partition.countRows(predicate))
    }).map(_.sum), Duration.Inf)
  }

  override def getRowStatistics: RowStatistics = {
    Await.result(Future.sequence(partitions map { partition =>
      Future(partition.getRowStatistics)
    }), Duration.Inf).reduce(_ + _)
  }

  override def readBlocks(rowID: ROWID, numberOfBlocks: ROWID): Seq[(ROWID, ByteBuffer)] = {
    // determine the partitions we need to read from
    val mappings = getReadPartitionMappings(rowID, numberOfBlocks)

    // asynchronously read the blocks
    val outcome = Future.sequence(mappings map { case (partition, offsets) =>
      Future(partition.readBlocks(offsets.head, numberOfBlocks = offsets.size))
    }) map (_.flatten)
    Await.result(outcome, Duration.Inf)
  }

  override def reverseInPlace(): Unit = {
    Await.result(Future.sequence(partitions map { partition =>
      Future(partition.reverseInPlace())
    }), Duration.Inf)
  }

  override def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): Unit = {
    case class Datum(partition: BlockDevice, offset: ROWID, buf: ByteBuffer)

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
  }

  private def getReadPartitionMappings(offset: ROWID, numberOfBlocks: ROWID): Seq[(BlockDevice, Seq[ROWID])] = {
    // determine the partitions we need to read from
    case class PartitionAndOffset(partition: BlockDevice, offset: ROWID)

    (for {
      globalOffset <- offset until offset + numberOfBlocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      localOffset = toLocalOffset(globalOffset, index)
    } yield PartitionAndOffset(partition, localOffset))
      .groupBy(_.partition)
      .map { case (partition, offsets) => partition -> offsets.map(_.offset) }
      .toSeq
  }

}
