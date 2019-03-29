package com.github.ldaniels528.qwery

import org.apache.spark.sql.api.java.UDF1

/**
  * Currency Converter Spark UDF example
  * @author lawrence.daniels@gmail.com
  */
class Currency() extends UDF1[String, String] {

  /**
    * Converts currency text to its decimal equivalent
    * @param value the given currency value (e.g. "$20.96B")
    * @return the option of a decimal value
    */
  override def call(value: String): String = {
    val number = value.filter(c => c.isDigit || c == '.')
    val result = if (number.isEmpty) 0d else {
      val factor = value.last.toUpper match {
        case 'K' => 1e+3
        case 'M' => 1e+6
        case 'B' => 1e+9
        case 'T' => 1e+12
        case 'P' => 1e+15
        case _ => 1d
      }
      factor * number.toDouble
    }
    result.toString
  }

}
