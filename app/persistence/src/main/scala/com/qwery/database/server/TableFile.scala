package com.qwery.database.server

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer.allocate

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.FileBlockDevice.getHeader
import com.qwery.database.server.TableQueryExecutor._
import com.qwery.database.{BlockDevice, Codec, Column, FileBlockDevice, ROWID, RowMetadata}

/**
 * Represents a database table
 * @param name   the name of the table
 * @param device the [[BlockDevice]]
 */
case class TableFile(name: String, device: BlockDevice) {

  def close(): Unit = device.close()

  def delete(rowID: Int): Unit = device.writeRowMetaData(rowID, RowMetadata(isActive = false))

  def get(rowID: Int): Result = {
    val row = device.getRow(rowID)
    if (row.metadata.isActive) for {field <- row.fields; value <- field.value} yield field.name -> value else Nil
  }

  def insert(values: (Symbol, Any)*): ROWID = {
    val rowID = device.length
    replace(rowID, values: _*)
    rowID
  }

  def replace(rowID: Int, values: (Symbol, Any)*): Unit = {
    val mapping = Map(values.map { case (k, v) => (k.name, v) }: _*)
    val buf = allocate(device.recordSize)
    buf.putRowMetadata(RowMetadata())
    device.columns zip device.columnOffsets foreach { case (col, offset) =>
      buf.position(offset)
      val value_? = mapping.get(col.name)
      buf.put(Codec.encode(col, value_?))
    }
    device.writeBlock(rowID, buf)
  }

  def search(limit: Option[Int], conditions: (Symbol, Any)*): List[Result] = {
    val condCached = conditions.map { case (symbol, value) => (symbol.name, value) }
    var results: List[Result] = Nil
    var rowID = 0
    val eof = device.length
    while (rowID < eof && (limit.isEmpty || limit.exists(_ > results.size))) {
      val result = get(rowID)
      if (isSatisfied(result, condCached) || condCached.isEmpty) results = result :: results
      rowID += 1
    }
    results
  }

  /**
   * Truncates the table; removing all rows
   */
  def truncate(): Unit = device.shrinkTo(newSize = 0)

  @inline
  private def isSatisfied(result: Result, condition: Result): Boolean = {
    val resultMap = Map(result: _*)
    condition.forall { case (name, value) => resultMap.get(name).contains(value) }
  }

}

/**
 * Table File Companion
 */
object TableFile {

  /**
   * Retrieves a table by name
   * @param name the name of the table
   * @return the [[TableFile]]
   */
  def apply(name: String): TableFile = {
    val file = getDataFile(name)
    val device = new FileBlockDevice(columns = getHeader(file).columns, file)
    TableFile(name, device)
  }

  /**
   * Creates a new database table
   * @param name the name of the table
   * @param columns the table columns
   * @return the new [[TableFile]]
   */
  def create(name: String, columns: Seq[Column]): TableFile = {
    val file = getDataFile(name)

    // create a new database file
    val raf = new RandomAccessFile(file, "rw")
    raf.seek(0)
    raf.write(BlockDevice.Header(columns).toBuffer.array())
    raf.close()

    // return the table
    TableFile(name, new FileBlockDevice(columns, file))
  }

  def getDataDirectory: File = {
    val dataDirectory = new File("qwerydb")
    if (!dataDirectory.mkdirs() && !dataDirectory.exists())
      throw new IllegalStateException(s"Could not create data directory - ${dataDirectory.getAbsolutePath}")
    dataDirectory
  }

  def getDataFile(name: String): File = new File(getDataDirectory, s"$name.qdb")

}