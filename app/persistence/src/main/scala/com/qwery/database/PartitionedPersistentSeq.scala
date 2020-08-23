package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import scala.reflect.ClassTag

/**
 * Represents a partitioned persistent sequence
 * @param partitionSize the size of each partition
 * @tparam T the product type
 */
class PartitionedPersistentSeq[T <: Product : ClassTag](partitionSize: Int) extends PersistentSeq[T]() {
  private var partitions: List[PersistentSeq[T]] = Nil
  assert(partitionSize > 0, "Partition size must be greater than zero")
  ensurePartitions(index = 1)

  override def close(): Unit = partitions.foreach(_.close())

  override def length: URID = partitions.map(_.length).sum

  override def shrinkTo(newSize: URID): PersistentSeq[T] = {
    partitions.foldLeft(newSize) { (size, partition) =>
      val remainder = Math.max(size - partition.length, 0)
      val myNewSize = Math.max(partition.length - size, 0)
      partition.shrinkTo(myNewSize)
      remainder
    }
    this
  }

  override def readBlock(offset: URID): ByteBuffer = {
    val index = toPartitionIndex(offset)
    val partition = partitions(index)
    partition.readBlock(toLocalOffset(offset, index))
  }

  override def readBlocks(offset: URID, numberOfBlocks: URID): Seq[(URID, ByteBuffer)] = {
    for {
      globalOffset <- offset to offset + numberOfBlocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      (_id, buf) <- partition.readBlocks(toLocalOffset(globalOffset, index))
    } yield (toGlobalOffset(_id, index), buf)
  }

  override def readByte(offset: URID): Byte = {
    val index = toPartitionIndex(offset)
    partitions(index).readByte(toLocalOffset(offset, index))
  }

  override def readBytes(offset: URID, numberOfBlocks: URID): Array[Byte] = {
    (for {
      globalOffset <- offset to offset + numberOfBlocks
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      (_, buf) <- partition.readBlocks(toLocalOffset(globalOffset, index))
    } yield buf.array()).toArray.flatten
  }

  override def writeBlocks(blocks: Seq[(URID, ByteBuffer)]): PersistentSeq[T] = {
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

  override def writeBytes(offset: URID, bytes: Array[Byte]): PersistentSeq[T] = {
    for {
      (globalOffset, buf) <- intoBlocks(offset, bytes)
      index = toPartitionIndex(globalOffset)
      partition = partitions(index)
      localOffset = toLocalOffset(globalOffset, index)
    } partition.writeBlock(localOffset, buf)
    this
  }

  override def writeByte(offset: URID, byte: URID): PersistentSeq[T] = {
    val index = toPartitionIndex(offset)
    partitions(index).writeByte(toLocalOffset(offset, index), byte)
  }

  private def ensurePartitions(index: Int): Unit = {
    while (partitions.size <= index) partitions = partitions ::: PersistentSeq[T]() :: Nil
  }

  private def intoBlocks(offset: URID, src: Array[Byte]): Seq[(URID, ByteBuffer)] = {
    val count = src.length / recordSize
    for (index <- 0 to count) yield {
      val buf = new Array[Byte](recordSize)
      System.arraycopy(src, index * recordSize, buf, 0, Math.min(buf.length, src.length - index * recordSize))
      (offset + index) -> wrap(buf)
    }
  }

  private def toGlobalOffset(offset: URID, index: Int): URID = offset + index * partitionSize

  private def toLocalOffset(offset: URID, index: Int): URID = Math.min(offset - index * partitionSize, partitionSize)

  private def toPartitionIndex(offset: Int, isLimit: Boolean = false): Int = {
    val index = offset / partitionSize
    val normalizedIndex = if (isLimit && offset == partitionSize) Math.min(0, index - 1) else index
    ensurePartitions(normalizedIndex)
    normalizedIndex
  }

}
