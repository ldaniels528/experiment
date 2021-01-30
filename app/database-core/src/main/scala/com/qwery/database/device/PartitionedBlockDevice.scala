package com.qwery.database.device

import com.qwery.database.{BinaryRow, Column, FieldMetadata, PartitionSizeException, ROWID, RowMetadata, createTempTable}

import java.nio.ByteBuffer

/**
 * Represents a partitioned block device
 * @param columns       the collection of [[Column columns]]
 * @param partitionSize the size of each partition
 */
class PartitionedBlockDevice(val columns: Seq[Column],
                             val partitionSize: Int,
                             isInMemory: Boolean = false) extends RowOrientedBlockDevice {
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

  override def readField(rowID: ROWID, columnID: Int): ByteBuffer = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.readField(toLocalOffset(rowID, index), columnID)
  }

  override def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata = {
    val index = toPartitionIndex(rowID)
    partitions(index).readFieldMetaData(toLocalOffset(rowID, index), columnID)
  }

  override def readRow(rowID: ROWID): BinaryRow = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.readRow(toLocalOffset(rowID, index))
  }

  override def readRowAsBinary(rowID: ROWID): ByteBuffer = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.readRowAsBinary(toLocalOffset(rowID, index))
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    val index = toPartitionIndex(rowID)
    partitions(index).readRowMetaData(toLocalOffset(rowID, index))
  }

  override def readRows(rowID: ROWID, numberOfRows: ROWID): Seq[BinaryRow] = {
    for {
      globalOffset <- rowID until rowID + numberOfRows
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      blocks <- partition.readRows(toLocalOffset(globalOffset, index))
        .map { case BinaryRow(_id, rmd, buf) => BinaryRow(toGlobalOffset(_id, index), rmd, buf) }
    } yield blocks
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    // determine the cut-off partition and overrun (remainder)
    val cutOffIndex = (newSize / partitionSize).toInt
    val remainder = newSize % partitionSize

    // adjust the size of the cut-off partition
    if (cutOffIndex < partitions.length) partitions(cutOffIndex).shrinkTo(remainder)

    // truncate the rest
    for {
      partition <- (cutOffIndex + 1) until partitions.length map partitions.apply
    } partition.shrinkTo(0)
  }

  override def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.writeField(toLocalOffset(rowID, index), columnID, buf)
  }

  override def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit = {
    val index = toPartitionIndex(rowID)
    partitions(index).writeFieldMetaData(toLocalOffset(rowID, index), columnID, metadata)
  }

  override def writeRowAsBinary(rowID: ROWID, buf: ByteBuffer): Unit = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.writeRowAsBinary(toLocalOffset(rowID, index), buf)
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    val index = toPartitionIndex(rowID)
    partitions(index).writeRowMetaData(toLocalOffset(rowID, index), metadata)
  }

  protected def ensurePartitions(index: Int): Unit = {
    while (partitions.size <= index) partitions = partitions ::: newPartition :: Nil
  }

  protected def newPartition: BlockDevice = {
    if (isInMemory) new ByteArrayBlockDevice(columns, capacity = partitionSize) else createTempTable(columns)
  }

  protected def toGlobalOffset(rowID: ROWID, index: Int): ROWID = rowID + index * partitionSize

  protected def toLocalOffset(rowID: ROWID, index: Int): ROWID = (rowID - index * partitionSize) min partitionSize

  protected def toPartitionIndex(offset: ROWID, isLimit: Boolean = false): Int = {
    val index = offset / partitionSize
    val normalizedIndex = (if (isLimit && offset == partitionSize) (index - 1) min 0 else index).toInt
    ensurePartitions(normalizedIndex)
    normalizedIndex
  }

}
