package com.qwery.database.device

import com.qwery.database.device.ExternalFileBlockDevice.DelimitedTextEnrichment
import com.qwery.database.files.DatabaseFiles.getTableDataFile
import com.qwery.database.files.TableColumn._
import com.qwery.database.files.TableConfig
import com.qwery.database.{BinaryRow, Column, FieldMetadata, KeyValues, ROWID, RecursiveFileList, RowMetadata, die}
import com.qwery.util.ResourceHelper._
import net.liftweb.json
import net.liftweb.json.{JNothing, JNull, JObject, JValue, _}

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
case class ExternalFileBlockDevice(databaseName: String, tableName: String, config: TableConfig) extends BlockDevice {
  // get the root file or directory
  private val rootFile: File = config.externalTable.flatMap(_.location.map(new File(_)))
    .getOrElse(die(s"No file or directory could be determine for table '$databaseName.$tableName'"))

  // setup the device
  private implicit val device: BlockDevice = new RowOrientedFileBlockDevice(columns, getTableDataFile(databaseName, tableName))
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

  override def columns: Seq[Column] = config.columns.map(_.toColumn)

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

  private def dieReadOnly(): Nothing = die(s"Table $databaseName.$tableName is read-only")

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
    format match {
      case "CSV" =>
        val values = line.delimitedSplit(',')
        KeyValues(columns.map(_.name) zip values: _*)
      case "JSON" =>
        unwrapJSON(json.parse(line)) match {
          case m: Map[String, Any] => KeyValues(m)
          case other => die(s"JSON object expected '$other'")
        }
      case other => die(s"Unrecognized format '$other'")
    }
  }

  private def unwrapJSON(jValue: JValue): Any = jValue match {
    case JArray(array) => array.map(unwrapJSON)
    case JBool(value) => value
    case JDouble(value) => value
    case JInt(value) => value
    case JNull | JNothing => null
    case js: JObject => js.values
    case JString(value) => value
    case x => die(s"Unsupported type $x (${x.getClass.getName})")
  }

}

/**
  * External File Block Device Companion
  */
object ExternalFileBlockDevice {

  /**
    * Delimited Text Enrichment
    * @param text the given Delimited text string
    */
  final implicit class DelimitedTextEnrichment(val text: String) extends AnyVal {

    def delimitedSplit(delimiter: Char): List[String] = {
      var inQuotes = false
      val sb = new StringBuilder()
      val values = text.toCharArray.foldLeft[List[String]](Nil) {
        case (list, ch) if ch == '"' =>
          inQuotes = !inQuotes
          //sb.append(ch)
          list
        case (list, ch) if inQuotes & ch == delimiter =>
          sb.append(ch); list
        case (list, ch) if !inQuotes & ch == delimiter =>
          val s = sb.toString().trim
          sb.clear()
          list ::: s :: Nil
        case (list, ch) =>
          sb.append(ch); list
      }
      if (sb.toString().trim.nonEmpty) values ::: sb.toString().trim :: Nil else values
    }
  }

}
