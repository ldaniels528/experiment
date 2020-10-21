package com.qwery.database.types

import java.io._

import com.qwery.database.PersistentSeq.newTempFile

/**
 * Character Large Object (CLOB)
 * @param raf the [[RandomAccessFile]]
 */
class Clob(raf: RandomAccessFile) {
  private var position: Long = 0L

  def getAsciiStream: InputStream = () => {
    val byte = raf.read()
    position += 1
    byte
  }

  def setAsciiStream(pos: Long): OutputStream = new OutputStream {
    position = pos
    override def write(byte: Int): Unit = {
      raf.seek(pos)
      raf.write(byte)
      position = pos + 1
    }
  }

  def getCharacterStream: Reader = new Reader {
    override def read(chars: Array[Char], off: Int, len: Int): Int = {
      val bytes = new Array[Byte](len)
      raf.seek(position)
      raf.read(bytes)
      val dbuf = new String(bytes).toCharArray
      System.arraycopy(dbuf, 0, chars, off, len)
      position += len
      dbuf.length
    }

    override def close(): Unit = ()
  }

  def getCharacterStream(pos: Long, length: Long): Reader = new Reader {
    override def read(chars: Array[Char], off: Int, len: Int): Int = {
      val bytes = new Array[Byte](len)
      raf.seek(pos)
      raf.read(bytes)
      val dbuf = new String(bytes).toCharArray
      System.arraycopy(dbuf, 0, chars, off, len)
      position = pos + len
      dbuf.length
    }

    override def close(): Unit = ()
  }

  def setCharacterStream(pos: Long): Writer = new Writer() {
    override def write(chars: Array[Char], off: Int, len: Int): Unit = {
      raf.seek(pos)
      raf.writeChars(new String(chars).substring(off, off + len))
      position = pos + len
    }

    override def flush(): Unit = ()

    override def close(): Unit = ()
  }

  def getSubString(pos: Long, length: Int): String = {
    raf.seek(pos)
    val bytes = new Array[Byte](length)
    raf.read(bytes)
    position = pos + length
    new String(bytes)
  }

  def setString(pos: Long, str: String): Int = {
    raf.seek(pos)
    val bytes = str.getBytes("utf-8")
    raf.write(bytes)
    position = pos + bytes.length
    bytes.length
  }

  def setString(pos: Long, str: String, offset: Int, len: Int): Int = {
    raf.seek(pos)
    val bytes = str.getBytes("utf-8")
    raf.write(bytes, offset, len)
    position = pos + len
    bytes.length
  }

  def free(): Unit = raf.close()

  def length(): Long = raf.length()

  def position(searchString: String, start: Long): Long = ??? // TODO do something about this some day

  def position(searchString: java.sql.Clob, start: Long): Long = ??? // TODO do something about this some day

  def truncate(len: Long): Unit = {
    raf.setLength(len)
    position = position min len
  }

}

object Clob {
  
  def create(): Clob = new Clob(new RandomAccessFile(newTempFile(), "rw"))

  def create(reader: Reader): Clob = {
    val bufReader = new BufferedReader(reader)
    val clob = new Clob(new RandomAccessFile(newTempFile(), "rw"))
    var pos = 0
    var line: String = null
    do {
      line = bufReader.readLine()
      if (line != null) {
        clob.setString(pos, line)
        pos += line.length
      }
    } while (line != null)
    clob
  }

  def create(reader: Reader, length: Long): Clob = {
    val bufReader = new BufferedReader(reader)
    val clob = new Clob(new RandomAccessFile(newTempFile(), "rw"))
    var pos = 0
    var line: String = null
    do {
      line = bufReader.readLine()
      if (line != null) {
        clob.setString(pos, line)
        pos += line.length
      }
    } while (line != null && pos < length)
    clob.truncate(length)
    clob
  }

  def load(file: File): Clob = new Clob(new RandomAccessFile(file, "rw"))

}