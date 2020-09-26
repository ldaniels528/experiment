package com.qwery.database.server

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer.allocate

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.FileBlockDevice.getHeader
import com.qwery.database.server.TableManager._
import com.qwery.database.{BlockDevice, Codec, Column, FileBlockDevice, ROWID, RowMetadata}
import com.qwery.util.ResourceHelper._

import scala.io.Source
import scala.language.postfixOps

/**
 * Represents a database table
 * @param name   the name of the table
 * @param device the [[BlockDevice]]
 */
case class TableFile(name: String, device: BlockDevice) {

  def close(): Unit = device.close()

  def count(): ROWID = device.countRows(_.isActive)

  def count(condition: Map[Symbol, Any], limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (_, _) => }
  }

  def delete(rowID: ROWID): Unit = {
    device.writeRowMetaData(rowID, RowMetadata(isActive = false))
  }

  def delete(condition: Map[Symbol, Any], limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (rowID, _) => delete(rowID) }
  }

  def find(condition: Map[Symbol, Any], limit: Option[Int] = None): List[Result] = {
    var results: List[Result] = Nil
    _iterate(condition, limit) { (_, result) => results = result :: results }
    results
  }

  def get(rowID: ROWID): Result = {
    val row = device.getRow(rowID)
    if (row.metadata.isActive) for {field <- row.fields; value <- field.value} yield field.name -> value else Nil
  }

  def insert(values: Map[Symbol, Any]): ROWID = {
    val rowID = device.length
    replace(rowID, values)
    rowID
  }

  def load(file: File)(transform: String => Map[Symbol, Any]): Long = {
    var nLines: Long = 0
    Source.fromFile(file).use(_.getLines() foreach { line =>
      val result = transform(line)
      if (result.nonEmpty) {
        insert(result)
        nLines += 1
      }
    })
    nLines
  }

  def replace(rowID: ROWID, values: Map[Symbol, Any]): Unit = {
    val mapping = values.map { case (k, v) => (k.name, v) }
    val buf = allocate(device.recordSize)
    buf.putRowMetadata(RowMetadata())
    device.columns zip device.columnOffsets foreach { case (col, offset) =>
      buf.position(offset)
      val value_? = mapping.get(col.name)
      buf.put(Codec.encode(col, value_?))
    }
    device.writeBlock(rowID, buf)
  }

  def slice(start: ROWID, length: ROWID): Seq[Result] = {
    var rows: List[Result] = Nil
    val limit = Math.min(device.length, start + length)
    var rowID = start
    while (rowID <= limit) {
      rows = get(rowID) :: rows
      rowID += 1
    }
    rows
  }

  def update(values: Map[Symbol, Any], condition: Map[Symbol, Any], limit: Option[Int] = None): Int = {
    _iterate(condition, limit) { (rowID, result) =>
      val updatedValues: Map[Symbol, Any] = Map(result.map { case (key, value) => (Symbol(key), value) }: _*) ++ values
      replace(rowID, updatedValues)
    }
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

  @inline
  private def _iterate(condition: Map[Symbol, Any], limit: Option[Int] = None)(f: (ROWID, Result) => Unit): Int = {
    val condCached = condition.map { case (symbol, value) => (symbol.name, value) } toSeq
    var matches: Int = 0
    var rowID: ROWID = 0
    val eof = device.length
    while (rowID < eof && !limit.exists(matches >= _)) {
      val result = get(rowID)
      if (isSatisfied(result, condCached) || condCached.isEmpty) {
        f(rowID, result)
        matches += 1
      }
      rowID += 1
    }
    matches
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
    if (!file.exists()) throw new IllegalArgumentException(s"Table '$name' does not exist")
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