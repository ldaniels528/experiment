package com.qwery.database

import java.nio.ByteBuffer

import com.qwery.database.PersistentSeq.newTempFile

/**
 * Represents a partitioned block device
 * @param columns       the collection of [[Column columns]]
 * @param partitionSize the size of each partition
 */
class PartitionedBlockDevice(val columns: Seq[Column],
                             val partitionSize: Int,
                             isInMemory: Boolean = false) extends BlockDevice {
  protected var partitions: List[BlockDevice] = Nil
  assert(partitionSize > 0, throw PartitionSizeException(partitionSize))
  //ensurePartitions(index = 1)

  def addPartition(partition: BlockDevice): this.type = {
    partitions = partition :: partitions
    this
  }

  override def close(): Unit = partitions.foreach(_.close())

  override def getPhysicalSize: Option[Long] = Some(partitions.flatMap(_.getPhysicalSize).sum)

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

  override def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata = {
    val index = toPartitionIndex(rowID)
    partitions(index).readFieldMetaData(toLocalOffset(rowID, index), columnID)
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    val index = toPartitionIndex(rowID)
    partitions(index).readRowMetaData(toLocalOffset(rowID, index))
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

  override def writeBytes(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.writeBytes(toLocalOffset(rowID, index), columnID, buf)
  }

  override def writeFieldMetaData(rowID: ROWID, columnID: ROWID, metadata: FieldMetadata): Unit = {
    val index = toPartitionIndex(rowID)
    partitions(index).writeFieldMetaData(toLocalOffset(rowID, index), columnID, metadata)
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    val index = toPartitionIndex(rowID)
    partitions(index).writeRowMetaData(toLocalOffset(rowID, index), metadata)
  }

  protected def ensurePartitions(index: Int): Unit = {
    while (partitions.size <= index) partitions = partitions ::: newPartition :: Nil
  }

  protected def newPartition: BlockDevice = {
    if (isInMemory) new ByteArrayBlockDevice(columns, capacity = partitionSize) else new FileBlockDevice(columns, newTempFile())
  }

  protected def toGlobalOffset(rowID: ROWID, index: Int): ROWID = rowID + index * partitionSize

  protected def toLocalOffset(rowID: ROWID, index: Int): ROWID = Math.min(rowID - index * partitionSize, partitionSize)

  protected def toPartitionIndex(offset: RECORD_ID, isLimit: Boolean = false): Int = {
    val index = offset / partitionSize
    val normalizedIndex = if (isLimit && offset == partitionSize) Math.min(0, index - 1) else index
    ensurePartitions(normalizedIndex)
    normalizedIndex
  }

}
