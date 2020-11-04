package com.qwery.database.device

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.{Column, FieldMetadata, MathUtilsLong, ROWID, RowMetadata}

import scala.language.postfixOps

/**
 * Column-Oriented File Block Device
 * @param columns the collection of [[Column columns]]
 * @param file    the persistence [[File file]]
 */
case class ColumnOrientedFileBlockDevice(columns: Seq[Column], file: File) extends ColumnOrientedBlockDevice {
  private val raf0 = new RandomAccessFile(file, "rw")
  private val rafN: Seq[(RandomAccessFile, Column)] = columns.filterNot(_.isLogical).zipWithIndex map { case (column, columnIndex) =>
    val fileName = file.getName
    val theFileName = fileName.lastIndexOf('.') match {
      case -1 => fileName + s"_$columnIndex.qbd"
      case n => new StringBuilder(fileName).replace(n, n + 1, s"_$columnIndex.").toString()
    }
    (new RandomAccessFile(new File(file.getParentFile, theFileName), "rw"), column)
  }

  override def close(): Unit = {
    raf0.close()
    rafN.foreach(_._1.close())
  }

  override def getPhysicalSize: Option[Long] = Some(raf0.length + rafN.map(_._1.length).sum)

  override def length: ROWID = raf0.length.toRowID

  override def readField(rowID: ROWID, columnID: Int): ByteBuffer = {
    val (raf, column) = rafN(columnID)
    val bytes = new Array[Byte](column.maxPhysicalSize)
    raf.seek(toOffset(rowID, column))
    raf.read(bytes)
    wrap(bytes)
  }

  override def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata = {
    val (raf, column) = rafN(columnID)
    raf.seek(toOffset(rowID, column))
    FieldMetadata.decode(raf.read().toByte)
  }

  override def readRow(rowID: ROWID): ByteBuffer = {
    val BinaryRow(_, rmd, fieldBufs) = readRowAsFields(rowID)
    val payload = allocate(recordSize)
    payload.putRowMetadata(rmd)
    fieldBufs.zipWithIndex foreach { case (fieldBuf, columnIndex) =>
      payload.position(columnOffsets(columnIndex))
      payload.put(fieldBuf)
    }
    payload.flip()
    payload
  }

  override def readRowAsFields(rowID: ROWID): BinaryRow = {
    val rmd = readRowMetaData(rowID)
    BinaryRow(rowID, rmd, fields = rafN map { case (raf, column) =>
      val columnBytes = new Array[Byte](column.maxPhysicalSize)
      raf.seek(toOffset(rowID, column))
      raf.read(columnBytes)
      wrap(columnBytes)
    })
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    raf0.seek(rowID)
    RowMetadata.decode(raf0.read().toByte)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < raf0.length()) {
      raf0.setLength(newSize)
      rafN.foreach { case (raf, column) => raf.setLength(toOffset(newSize, column)) }
    }
  }

  override def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit = {
    val (raf, column) = rafN(columnID)
    raf.seek(toOffset(rowID, column))
    raf.write(buf.array())
  }

  override def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit = {
    val (raf, column) = rafN(columnID)
    raf.seek(toOffset(rowID, column))
    raf.write(metadata.encode)
  }

  override def writeRow(rowID: ROWID, buf: ByteBuffer): Unit = {
    writeRowMetaData(rowID, RowMetadata())
    rafN.zipWithIndex foreach { case ((raf, column), columnIndex) =>
      val columnBytes = new Array[Byte](column.maxPhysicalSize)
      buf.position(columnOffsets(columnIndex))
      buf.get(columnBytes)
      raf.seek(toOffset(rowID, column))
      raf.write(columnBytes)
    }
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    raf0.seek(rowID)
    raf0.write(metadata.encode)
  }

}