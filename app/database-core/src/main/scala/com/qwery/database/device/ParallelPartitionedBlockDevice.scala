package com.qwery.database.device

import com.qwery.database.models.{BinaryRow, TableColumn, RowMetadata, RowStatistics}
import com.qwery.database.{ROWID, models}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Parallel Partitioned Block Device
 * @param columns       the collection of [[TableColumn columns]]
 * @param partitionSize the size of each partition
 * @param ec            the implicit [[ExecutionContext]]
 */
class ParallelPartitionedBlockDevice(columns: Seq[TableColumn],
                                     partitionSize: Int,
                                     isInMemory: Boolean = false)(implicit ec: ExecutionContext)
  extends PartitionedBlockDevice(columns, partitionSize, isInMemory) {

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

  override def readRows(rowID: ROWID, numberOfRows: ROWID): Seq[BinaryRow] = {
    // determine the partitions we need to read from
    val mappings = getReadPartitionMappings(rowID, numberOfRows)

    // asynchronously read the blocks
    val outcome = Future.sequence(mappings map { case (partition, offsets) =>
      Future(partition.readRows(offsets.head, numberOfRows = offsets.size))
    }) map (_.flatten)
    Await.result(outcome, Duration.Inf)
  }

  override def reverseInPlace(): Unit = {
    Await.result(Future.sequence(partitions map { partition =>
      Future(partition.reverseInPlace())
    }), Duration.Inf)
  }

  override def writeRows(rows: Seq[BinaryRow]): Unit = {
    case class Datum(partition: BlockDevice, row: BinaryRow)

    // determine the partitions we need to write to
    val results =
      (for {
        BinaryRow(globalOffset, rmd, buf) <- rows
        index = toPartitionIndex(globalOffset)
        partition = partitions(index)
        localOffset = toLocalOffset(globalOffset, index)
      } yield Datum(partition, models.BinaryRow(localOffset, rmd, buf))).groupBy(_.partition).toSeq

    // asynchronously write the blocks
    val outcome = Future.sequence(results map { case (partition, datum) =>
      Future(partition.writeRows(datum.map { case Datum(_, row) => row }))
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
