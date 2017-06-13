package com.github.ldaniels528.transgress.client

import com.github.ldaniels528.transgress.BytesHelper._

import scala.scalajs.js

/**
  * Bourne Filters
  * @author lawrence.daniels@gmail.com
  */
object Filters {
  private val timeUnits = Seq("sec", "min", "hour", "day", "month", "year")
  private val timeFactors = Seq(60, 60, 24, 30, 12)

  /**
    * Duration: Converts a given time stamp to a more human readable expression (e.g. "5 mins ago")
    */
  val duration: js.Function = () => { (time: js.UndefOr[Double]) => toDuration(time) }: js.Function

  val bytes: js.Function = () => { (aValue: js.UndefOr[Double]) => aValue.map(_.bytes) }: js.Function

  val bps: js.Function = () => { (aValue: js.UndefOr[Double]) => aValue.map(_.bps) }: js.Function

  /**
    * Converts the given time expression to a textual duration
    * @param aTime the given [[js.Date]] or time stamp (in milliseconds)
    * @return the duration (e.g. "10 mins ago")
    */
  def toDuration(aTime: js.UndefOr[Double]): js.UndefOr[String] = aTime map { time =>
    // compute the age
    var age0 = time
    var age1 = 0.0

    var unit = 0
    while (unit < timeFactors.length && age0 >= timeFactors(unit)) {
      val timeFactor = timeFactors(unit)
      val age = age0
      age0 = age / timeFactor
      age1 = age % timeFactor
      unit += 1
    }

    // make the age and unit names more readable
    val unitName0 = timeUnits(unit) + (if (age0 != 1.0) "s" else "")
    val unitName1 = if (unit > 0) timeUnits(unit - 1) + (if (age1 != 1.0) "s" else "") else ""
    if (age1 == 0.0) f"$age0%.0f $unitName0" else f"$age0%.0f $unitName0, $age1%.0f $unitName1"
  }

}
