package com.github.ldaniels528.transgress

import scala.annotation.tailrec

/**
  * Bytes Helper
  * @author lawrence.daniels@gmail.com
  */
object BytesHelper {
  private val UnitNames = Seq("Bytes", "KB", "MB", "GB", "TB", "PB")

  /**
    * Byte Unit Enrichment
    * @param value the given byte size
    */
  implicit class BytesUnitEnrichment(val value: Double) extends AnyVal {

    @inline
    def bytes: String = units(value)

    @inline
    def bps: String = units(value) + "/sec"

    @tailrec
    private def units(value: Double, unit: Int = 0): String = {
      if (value >= 1000) units(value / 1024, unit + 1) else f"$value%.2f ${UnitNames(unit)}"
    }

  }
}
