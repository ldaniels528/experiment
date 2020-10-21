package com.qwery.database
package jdbc

import java.io._
import java.sql.SQLXML

import com.qwery.database.PersistentSeq.newTempFile
import javax.xml.transform.{Result, Source}

/**
 * SQL/XML Text
 * @param raf the [[RandomAccessFile]]
 */
class JDBCSQLXML(raf: RandomAccessFile) extends SQLXML {
  private var position: Long = 0L

  override def free(): Unit = raf.close()

  override def getBinaryStream: InputStream = () => {
    raf.seek(position)
    val b = raf.read()
    position += 1
    b
  }

  override def setBinaryStream(): OutputStream = (b: Int) => {
    raf.seek(position)
    raf.write(b)
    position += 1
  }

  override def getCharacterStream: Reader = new Reader {
    raf.seek(0)
    override def read(cbuf: Array[Char], off: Int, len: Int): Int = {
      val bytes = new Array[Byte](len)
      val count = raf.read(bytes)
      val string = new String(bytes, 0, count)
      System.arraycopy(string.toCharArray, 0, cbuf, off, len)
      position += count
      count
    }

    override def close(): Unit = ()
  }

  override def setCharacterStream(): Writer = new Writer {
    override def write(cbuf: Array[Char], off: Int, len: Int): Unit = {
      val bytes = String.copyValueOf(cbuf, off, len).getBytes("utf-8")
      raf.seek(position)
      raf.write(bytes)
      position += bytes.length
    }

    override def flush(): Unit = ()

    override def close(): Unit = ()
  }

  override def getString: String = {
    val bytes = new Array[Byte](raf.length.toInt)
    raf.seek(0)
    raf.read(bytes)
    position += bytes.length
    new String(bytes)
  }

  override def setString(value: String): Unit = {
    val bytes = value.getBytes("utf-8")
    raf.seek(0)
    raf.write(bytes)
    position += bytes.length
  }

  override def getSource[T <: Source](sourceClass: Class[T]): T = ???

  override def setResult[T <: Result](resultClass: Class[T]): T = ???

}

/**
 * JDBC SQL/XML Companion
 */
object JDBCSQLXML {

  def create(): JDBCSQLXML = new JDBCSQLXML(new RandomAccessFile(newTempFile(), "rw"))

  def load(file: File): JDBCSQLXML = new JDBCSQLXML(new RandomAccessFile(file, "rw"))

}