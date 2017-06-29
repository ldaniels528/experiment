package com.github.ldaniels528.transgress

import io.scalajs.nodejs.os.OS
import io.scalajs.nodejs.setTimeout

import scala.concurrent.{Future, Promise}
import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * CPU Monitor - How to calculate the current CPU load with Node.js without using any external modules or OS specific calls.
  * @see https://gist.github.com/bag-man/5570809
  */
object CpuMonitor {

  def computeLoad(): Future[Double] = {
    val promise = Promise[Double]()

    // Grab first CPU Measure
    val startMeasure = cpuAverage()

    // Set delay for second Measure
    setTimeout(() => {
      // Grab second Measure
      val endMeasure = cpuAverage()

      // Calculate the difference in idle and total time between the measures
      val idleDifference = endMeasure.idle - startMeasure.idle
      val totalDifference = endMeasure.total - startMeasure.total

      // Calculate the average percentage CPU usage
      val cpu_% = 100 - Math.floor(100 * idleDifference / totalDifference)

      // Output result to console
      promise.success(cpu_%)
    }, 100)

    promise.future
  }

  /**
    * Create function to get CPU information
    */
  private def cpuAverage(): CPUTicks = {
    val cpus = OS.cpus()
    new CPUTicks(
      idle = cpus.map(_.times("idle")).sum / cpus.length,
      total = cpus.flatMap(_.times.map(_._2)).sum / cpus.length)
  }

  @ScalaJSDefined
  class CPUTicks(val idle: Double, val total: Double) extends js.Object

}
