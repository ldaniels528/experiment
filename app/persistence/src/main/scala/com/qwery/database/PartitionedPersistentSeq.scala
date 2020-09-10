package com.qwery.database

import java.nio.ByteBuffer

import scala.reflect.ClassTag

/**
 * Represents a partitioned persistent sequence
 * @param partitionSize the size of each partition
 * @tparam T the product type
 */
class PartitionedPersistentSeq[T <: Product : ClassTag](val partitionSize: Int) extends PersistentSeq[T]() {
  protected var partitions: List[PersistentSeq[T]] = Nil
  assert(partitionSize > 0, "Partition size must be greater than zero")
  //ensurePartitions(index = 1)

  def addPartition(partition: PersistentSeq[T]): this.type = {
    partitions = partition :: partitions
    this
  }

  override def close(): Unit = partitions.foreach(_.close())

  override def length: ROWID = partitions.map(_.length).sum

  override def readBlock(offset: ROWID): ByteBuffer = {
    val index = toPartitionIndex(offset)
    val partition = partitions(index)
    partition.readBlock(toLocalOffset(offset, index))
  }

  override def readBlocks(offset: ROWID, numberOfBlocks: ROWID): Seq[(ROWID, ByteBuffer)] = {
    for {
      globalOffset <- offset to offset + numberOfBlocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      blocks <- partition.readBlocks(toLocalOffset(globalOffset, index))
        .map { case (_id, buf) => toGlobalOffset(_id, index) -> buf }
    } yield blocks
  }

  override def shrinkTo(newSize: ROWID): PersistentSeq[T] = {
    partitions.foldLeft(newSize) { (size, partition) =>
      val remainder = Math.max(size - partition.length, 0)
      val myNewSize = Math.max(partition.length - size, 0)
      partition.shrinkTo(myNewSize)
      remainder
    }
    this
  }

  override def readByte(offset: ROWID): Byte = {
    val index = toPartitionIndex(offset)
    partitions(index).readByte(toLocalOffset(offset, index))
  }

  override def readBytes(offset: ROWID, numberOfBlocks: ROWID): Array[Byte] = {
    (for {
      globalOffset <- offset to offset + numberOfBlocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      (_, buf) <- partition.readBlocks(toLocalOffset(globalOffset, index))
    } yield buf.array()).toArray.flatten
  }

  override def readFragment(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): Array[Byte] = {
    val index = toPartitionIndex(rowID)
    val partition = partitions(index)
    partition.readFragment(toLocalOffset(rowID, index), numberOfBytes, offset)
  }

  override def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): PersistentSeq[T] = {
    for  {
      (globalOffset, buf) <- blocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      localOffset = toLocalOffset(globalOffset, index)
    } {
      partition.writeBlock(localOffset, buf)
    }
    this
  }

  override def writeBytes(offset: ROWID, bytes: Array[Byte]): PersistentSeq[T] = {
    for {
      (globalOffset, buf) <- intoBlocks(offset, bytes)
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      localOffset = toLocalOffset(globalOffset, index)
    } partition.writeBlock(localOffset, buf)
    this
  }

  override def writeByte(offset: ROWID, byte: ROWID): PersistentSeq[T] = {
    val index = toPartitionIndex(offset)
    partitions(index).writeByte(toLocalOffset(offset, index), byte)
  }

  private def ensurePartitions(index: Int): Unit = {
    while (partitions.size <= index) partitions = partitions ::: PersistentSeq[T]() :: Nil
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
