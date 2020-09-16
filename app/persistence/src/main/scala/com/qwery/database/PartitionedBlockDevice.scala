package com.qwery.database

import java.nio.ByteBuffer

import com.qwery.database.PersistentSeq.newTempFile

/**
 * Represents a partitioned block device
 * @param columns       the collection of [[Column columns]]
 * @param partitionSize the size of each partition
 */
class PartitionedBlockDevice(val columns: List[Column], val partitionSize: Int) extends BlockDevice {
  protected var partitions: List[BlockDevice] = Nil
  assert(partitionSize > 0, "Partition size must be greater than zero")
  //ensurePartitions(index = 1)

  def addPartition(partition: BlockDevice): this.type = {
    partitions = partition :: partitions
    this
  }

  override def close(): Unit = partitions.foreach(_.close())

  override def length: ROWID = partitions.map(_.length).sum

  override def readBlock(rowID: ROWID): ByteBuffer = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.readBlock(toLocalOffset(rowID, index))
  }

  override def readBlocks(rowID: ROWID, numberOfBlocks: ROWID): Seq[(ROWID, ByteBuffer)] = {
    for {
      globalOffset <- rowID until rowID + numberOfBlocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      blocks <- partition.readBlocks(toLocalOffset(globalOffset, index))
        .map { case (_id, buf) => toGlobalOffset(_id, index) -> buf }
    } yield blocks
  }

  override def readByte(rowID: ROWID): Byte = {
    val index = toPartitionIndex(rowID)
    partitions(index).readByte(toLocalOffset(rowID, index))
  }

  override def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.readBytes(toLocalOffset(rowID, index), numberOfBytes, offset)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    // determine the cut-off partition and overrun (remainder)
    val cutOffIndex = newSize / partitionSize
    val remainder = newSize % partitionSize

    // adjust the size of the cut-off partition
    if (cutOffIndex < partitions.length) partitions(cutOffIndex).shrinkTo(remainder)

    // truncate the rest
    for {
      partition <- (cutOffIndex + 1) until partitions.length map partitions.apply
    } partition.shrinkTo(0)
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.writeBlock(toLocalOffset(rowID, index), buf)
  }

  override def writeByte(rowID: ROWID, byte: Int): Unit = {
    val index = toPartitionIndex(rowID)
    partitions(index).writeByte(toLocalOffset(rowID, index), byte)
  }

  private def ensurePartitions(index: Int): Unit = {
    while (partitions.size <= index) partitions = partitions ::: new FileBlockDevice(columns, newTempFile()) :: Nil
  }

  protected def toGlobalOffset(offset: ROWID, index: Int): ROWID = offset + index * partitionSize

  protected def toLocalOffset(offset: ROWID, index: Int): ROWID = Math.min(offset - index * partitionSize, partitionSize)

  protected def toPartitionIndex(offset: Int, isLimit: Boolean = false): Int = {
    val index = offset / partitionSize
    val normalizedIndex = if (isLimit && offset == partitionSize) Math.min(0, index - 1) else index
    ensurePartitions(normalizedIndex)
    normalizedIndex
  }

}
