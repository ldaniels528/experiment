package com.qwery.database

import java.io.File

/**
 * Qwery server package object
 */
package object server {

  // miscellaneous constants
  val DEFAULT_DATABASE = "default"
  val ROW_ID_NAME = "__rowID"

  /**
   * Represents a row; a collection of tuples
   */
  type TupleSet = Map[String, Any]

  /**
   * Executes the block capturing its execution the time in milliseconds
   * @param block the block to execute
   * @return a tuplee containing the result of the block and its execution the time in milliseconds
   */
  def time[A](block: => A): (A, Double) = {
    val startTime = System.nanoTime()
    val result = block
    val finishTime = System.nanoTime()
    val elapsedTime = (finishTime - startTime) / 1e+6
    (result, elapsedTime)
  }

  final implicit class QweryFile(val theFile: File) extends AnyVal {

    /**
     * Recursively retrieves all files
     * @return the list of [[File files]]
     */
    def listFilesRecursively: List[File] = theFile match {
      case directory if directory.isDirectory => directory.listFiles().toList.flatMap(_.listFilesRecursively)
      case file => file :: Nil
    }

  }

}
