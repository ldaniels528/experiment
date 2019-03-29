package com.github.ldaniels528.qwery

import org.apache.spark.sql.api.java.UDF1

/**
  * NullFix Spark UDF example
  * @author lawrence.daniels@gmail.com
  */
class NullFix() extends UDF1[String, String] {

  override def call(value: String): String = value match {
    case "n/a" => null
    case s => s
  }

}
