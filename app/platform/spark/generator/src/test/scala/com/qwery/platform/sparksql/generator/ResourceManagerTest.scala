package com.qwery.platform.sparksql.generator

import com.qwery.models.{Column, StorageFormats, Table}
import com.qwery.util.StringHelper._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSpec

/**
  * Resource Manager Test Suite
  * @author lawrence.daniels@gmail.com
  */
class ResourceManagerTest extends FunSpec {

  describe(ResourceManager.getObjectSimpleName) {

    it("should dynamically create data frames") {
      implicit val spark: SparkSession = createSparkSession(appName = "Test App")

      // create a table
      ResourceManager.add(Table(
        name = "company_list",
        columns = List("customer_id STRING", "name STRING", "address STRING", "ingestion_date LONG").map(Column.apply),
        location = "./samples/companylist/csv/",
        fieldDelimiter = None,
        fieldTerminator = None,
        headersIncluded = None,
        nullValue = None,
        inputFormat = Some(StorageFormats.CSV),
        outputFormat = Some(StorageFormats.CSV),
        partitionColumns = Nil,
        properties = Map(),
        serdeProperties = Map("quoteChar" -> "\"", "separatorChar" -> ",")
      ))

      ResourceManager.read("company_list").show(5)
    }
  }

  def createSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config(new SparkConf())
      .enableHiveSupport()
      .getOrCreate()
  }

}
