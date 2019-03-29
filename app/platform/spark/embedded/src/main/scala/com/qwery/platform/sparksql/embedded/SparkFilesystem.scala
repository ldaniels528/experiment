package com.qwery.platform.sparksql.embedded

import java.io.File

import com.qwery.platform.sparksql.embedded.SparkFilesystem._
import org.apache.spark.sql.DataFrame

/**
  * Retrieves local files
  * @param path the local file path
  */
case class SparkFilesystem(path: String) extends SparkInvokable {
  override def execute(input: Option[DataFrame])(implicit rc: EmbeddedSparkContext): Option[DataFrame] = Option(getFilesAsDf(path))
}

/**
  * Spark Filesystem Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkFilesystem {

  /**
    * Retrieves a recursive collection of files as rows of a data frame
    * @param path the given local file path
    * @return a [[DataFrame data frame]]
    */
  def getFilesAsDf(path: String)(implicit rc: EmbeddedSparkContext): DataFrame = {
    import rc.spark.implicits._

    (getFiles(new File(path)) map { f =>
      (f.getName, f.getAbsolutePath, f.length(), f.canExecute, f.canRead, f.canWrite, f.getParent, f.isDirectory, f.isFile, f.isHidden)
    }) toDF("name", "absolutePath", "length", "canExecute", "canRead", "canWrite", "parent", "isDirectory", "isFile", "isHidden")
  }

  /**
    * Retrieves a recursive collection of files
    * @param file the root file or directory
    * @return a collection of [[File file]]s
    */
  def getFiles(file: File): Stream[File] = {
    file match {
      case d if d.isDirectory => d.listFiles().toStream.flatMap(getFiles)
      case f => Stream(f)
    }
  }
}