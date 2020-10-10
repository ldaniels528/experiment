package com.qwery.database
package jdbc

import java.io.{BufferedReader, File, InputStream, OutputStream, RandomAccessFile, Reader, Writer}
import java.sql.{Blob, Clob, NClob}

import com.qwery.database.PersistentSeq.newTempFile

/**
 * Qwery Binary Large Object (BLOB)
 * @param raf the [[RandomAccessFile]]
 */
class JDBCBlob(raf: RandomAccessFile) extends Blob with Clob with NClob {
  private var position: Long = 0L

  override def free(): Unit = raf.close()

  override def getAsciiStream: InputStream = () => {
    val byte = raf.read()
    position += 1
    byte
  }

  override def setAsciiStream(pos: Long): OutputStream = new OutputStream {
    position = pos
    override def write(byte: Int): Unit = {
      raf.seek(pos)
      raf.write(byte)
      position = pos + 1
    }
  }

  override def getBinaryStream: InputStream = () => {
    val byte = raf.read()
    position += 1
    byte
  }

  override def getBinaryStream(pos: Long, length: Long): InputStream = new InputStream {
    position = pos
    override def read(): Int = {
      raf.seek(pos)
      val byte = raf.read()
      position += 1
      byte
    }
  }

  override def setBinaryStream(pos: Long): OutputStream = new OutputStream {
    position = pos
    override def write(byte: Int): Unit = {
      raf.seek(pos)
      raf.write(byte)
      position = pos + 1
    }
  }

  override def getBytes(pos: Long, length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    raf.read(bytes, pos.toInt, length)
    position = pos + length
    bytes
  }

  override def getCharacterStream: Reader = new Reader {
    override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
      val bytes = new Array[Byte](len)
      raf.seek(position)
      raf.read(bytes)
      val dbuf = new String(bytes).toCharArray
      System.arraycopy(dbuf, 0, cbuf, off, len)
      position += len
      dbuf.length
    }

    override def close(): Unit = ()
  }

  override def getCharacterStream(pos: Long, length: Long): Reader = new Reader {
    override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
      val bytes = new Array[Byte](len)
      raf.seek(pos)
      raf.read(bytes)
      val dbuf = new String(bytes).toCharArray
      System.arraycopy(dbuf, 0, cbuf, off, len)
      position = pos + len
      dbuf.length
    }

    override def close(): Unit = ()
  }

  override def setCharacterStream(pos: Long): Writer = new Writer() {
    override def write(cbuf: Array[Char], off: Int, len: Int): Unit = {
      raf.seek(pos)
      raf.writeChars(new String(cbuf).substring(off, off + len))
      position = pos + len
    }

    override def flush(): Unit = ()

    override def close(): Unit = ()
  }

  override def length(): Long = raf.length()

  override def position(pattern: Array[Byte], start: Long): Long = -1 // TODO do something about this some day

  override def position(pattern: Blob, start: Long): Long = -1 // TODO do something about this some day

  override def setBytes(pos: Long, bytes: Array[Byte]): Int = {
    raf.seek(pos)
    raf.write(bytes)
    position = pos + bytes.length
    bytes.length
  }

  override def setBytes(pos: Long, bytes: Array[Byte], offset: Int, len: Int): Int = {
    raf.seek(pos)
    raf.write(bytes, offset, len)
    position = pos + len
    len
  }

  override def truncate(len: Long): Unit = {
    raf.setLength(len)
    position = position min len
  }

  override def getSubString(pos: Long, length: Int): String = {
    raf.seek(pos)
    val bytes = new Array[Byte](length)
    raf.read(bytes)
    position = pos + length
    new String(bytes)
  }

  override def position(searchString: String, start: Long): Long = -1 // TODO do something about this some day

  override def position(searchString: Clob, start: Long): Long = -1 // TODO do something about this some day

  override def setString(pos: Long, str: String): Int = {
    raf.seek(pos)
    val bytes = str.getBytes("utf-8")
    raf.write(bytes)
    position = pos + bytes.length
    bytes.length
  }

  override def setString(pos: Long, str: String, offset: Int, len: Int): Int = {
    raf.seek(pos)
    val bytes = str.getBytes("utf-8")
    raf.write(bytes, offset, len)
    position = pos + len
    bytes.length
  }

}

/**
 * Qwery Blob Companion
 */
object JDBCBlob {

  def create(): JDBCBlob = new JDBCBlob(new RandomAccessFile(newTempFile(), "rw"))

  def create(inputStream: InputStream): JDBCBlob = {
    ???
  }

  def create(inputStream: InputStream, length: Long): JDBCBlob = {
    ???
  }

  def create(reader: Reader): JDBCBlob = {
    val bufReader = new BufferedReader(reader)
    val blob = new JDBCBlob(new RandomAccessFile(newTempFile(), "rw"))
    var pos = 0
    var line: String = null
    do {
      line = bufReader.readLine()
      if (line != null) {
        blob.setString(pos, line)
        pos += line.length
      }
    } while (line != null)
    blob
  }

  def create(reader: Reader, length: Long): JDBCBlob = {
    ???
  }

  def load(file: File): JDBCBlob = new JDBCBlob(new RandomAccessFile(file, "rw"))

}
