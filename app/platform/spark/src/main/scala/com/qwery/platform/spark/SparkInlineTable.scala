package com.qwery.platform.spark

import com.qwery.models.{Column, TableLike}

/**
  * Spark Inline Table
  * @param name    the name of the table
  * @param columns the collection of table [[Column column]]s
  * @param source  the [[SparkInvokable]]
  */
case class SparkInlineTable(name: String, columns: Seq[Column], source: SparkInvokable) extends TableLike