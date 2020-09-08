package com.qwery.util

/**
 * Servicing Tools
 */
object ServicingTools {

  /**
   * Executes the block capturing the execution time
   * @param block the block to execute
   * @tparam T the result type
   * @return a tuple containing the result and execution time in milliseconds
   */
  def time[T](block: => T): (T, Double) = {
    val startTime = System.nanoTime()
    val result = block
    val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
    (result, elapsedTime)
  }

}
