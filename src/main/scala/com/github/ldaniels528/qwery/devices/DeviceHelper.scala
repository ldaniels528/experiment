package com.github.ldaniels528.qwery.devices

import java.io.{File, RandomAccessFile}

/**
  * Device Helper
  * @author lawrence.daniels@gmail.com
  */
object DeviceHelper {

  def getFileSize(file: File): Long = {
    if (file.getName.toLowerCase.endsWith(".gz")) getUncompressedSize(file)
    else file.length()
  }

  def getUncompressedSize(file: File): Long = {
    val raf = new RandomAccessFile(file, "r")
    raf.seek(raf.length - 4)
    val b4 = raf.read
    val b3 = raf.read
    val b2 = raf.read
    val b1 = raf.read
    (b1.toLong << 24) | (b2.toLong << 16) | (b3.toLong << 8) | b4.toLong
  }

  implicit class FileEnrichment(val file: File) extends AnyVal {

    /**
      * Returns the file name without the extension (e.g. "companylist.csv" => "companylist)
      * @return the file name without the extension
      */
    @inline
    def getBaseName: String = {
      val name = file.getName
      name.lastIndexOf('.') match {
        case -1 => name
        case index => name.substring(0, index)
      }
    }

    /**
      * Return the file size
      * @return the file size
      */
    @inline
    def getSize: Long = getFileSize(file)

  }

}
