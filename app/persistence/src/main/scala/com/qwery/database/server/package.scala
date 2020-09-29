package com.qwery.database

/**
 * Qwery server package object
 */
package object server {

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

}
