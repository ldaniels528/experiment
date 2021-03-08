package com.qwery.database
package device

import com.qwery.database.files.DatabaseFiles.getTableDataFile
import com.qwery.database.models.{BinaryRow, Column, FieldMetadata, JsValueConversion, KeyValues, RowMetadata, TableConfig}
import com.qwery.models.EntityRef
import com.qwery.util.ResourceHelper._

import java.io.File
import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap
import scala.io.Source

/**
  * External File Block Device
  * @param databaseName the database name
  * @param tableName    the table name
  * @param config       the [[TableConfig table configuration]]
  */
case class ExternalFileBlockDevice(ref: EntityRef, config: TableConfig) extends BlockDevice {
  // get the root file or directory
  private val rootFile: File = config.externalTable.flatMap(_.location.map(new File(_)))
    .getOrElse(die(s"No file or directory could be determine for table '${ref.toSQL}'"))

  // setup the device
  private implicit val device: BlockDevice = new RowOrientedFileBlockDevice(columns, getTableDataFile(ref, config))
  device.shrinkTo(newSize = 0)

  // get the config details
  val format: String = config.externalTable.flatMap(_.format).getOrElse(die("No format specified"))
  val nullValue: Option[String] = config.externalTable.flatMap(_.nullValue)

  // internal variables
  private val files: Iterator[File] = rootFile.streamFilesRecursively.toIterator
  private val physicalSizeCache = TrieMap[Unit, Option[ROWID]]()
  private var offset: ROWID = 0

  // ensure a page of data
  assert(files.hasNext, s"No files found in '${rootFile.getCanonicalPath}'")
  ensureData(rowID = 20)

  override def close(): Unit = device.close()

  override def columns: Seq[Column] = config.columns

  override def getPhysicalSize: Option[ROWID] = physicalSizeCache.getOrElseUpdate((), {
    val list = rootFile.listFilesRecursively.map(_.length())
    if (list.nonEmpty) Some(list.sum) else None
  })

  override def length: ROWID = {
    ensureData(rowID = Long.MaxValue)
    device.length
  }

  override def readField(rowID: ROWID, columnID: Int): ByteBuffer = {
    ensureData(rowID)
    device.readField(rowID, columnID)
  }

  override def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata = {
    ensureData(rowID)
    device.readFieldMetaData(rowID, columnID)
  }

  override def readRowAsBinary(rowID: ROWID): ByteBuffer = {
    ensureData(rowID)
    device.readRowAsBinary(rowID)
  }

  override def readRow(rowID: ROWID): BinaryRow = {
    ensureData(rowID)
    device.readRow(rowID)
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    ensureData(rowID)
    device.readRowMetaData(rowID)
  }

  override def shrinkTo(newSize: ROWID): Unit = dieReadOnly()

  override def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit = dieReadOnly()

  override def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit = dieReadOnly()

  override def writeRowAsBinary(rowID: ROWID, buf: ByteBuffer): Unit = dieReadOnly()

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = dieReadOnly()

  private def dieReadOnly(): Nothing = die(s"Table '${ref.toSQL}' is read-only")

  private def ensureData(rowID: ROWID): Unit = {
    while (offset <= rowID && files.hasNext) {
      val file = files.next()
      Source.fromFile(file).use(_.getLines() foreach { line =>
        val keyValues = parseText(line)
        device.writeRow(keyValues.toBinaryRow(offset))
        offset += 1
      })
    }
  }

  private def parseText(line: String): KeyValues = {
    format.toUpperCase match {
      case "CSV" =>
        val values = line.delimitedSplit(',')
        val pairs = columns.map(_.name) zip values flatMap {
          case (_, s) if nullValue.contains(s) => None
          case x => Some(x)
        }
        KeyValues(pairs: _*)
      case "JSON" =>
        import spray.json._
        line.parseJson.unwrapJSON match {
          case m: Map[String, Any] => KeyValues(m)
          case other => die(s"JSON object expected '$other'")
        }
      case other => die(s"Unrecognized format '$other'")
    }
  }

}