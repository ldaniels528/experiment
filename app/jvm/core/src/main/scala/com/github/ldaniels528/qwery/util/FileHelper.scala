package com.github.ldaniels528.qwery.util

import java.io.{File, FilenameFilter}

/**
  * File Helper
  * @author lawrence.daniels@gmail.com
  */
object FileHelper {

  /**
    * Returns a recursive collection files starting with the current directory
    * @param file the given [[File file]]
    * @return a stream of files
    */
  def getFiles(file: File): Stream[File] = file match {
    case f if f.isDirectory => f.listFiles().toStream.flatMap(getFiles)
    case f => Stream(f)
  }

  /**
    * Returns a recursive collection files starting with the current directory
    * @param file the given [[File file]]
    * @return a stream of files
    */
  def getFiles(file: File, filter: FilenameFilter): Stream[File] = file match {
    case f if f.isDirectory => f.listFiles(filter).toStream.flatMap(getFiles)
    case f => Stream(f)
  }

}
