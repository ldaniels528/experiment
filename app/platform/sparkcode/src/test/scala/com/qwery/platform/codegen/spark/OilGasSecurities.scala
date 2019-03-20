package com.qwery.platform.codegen.spark

import com.qwery.models.StorageFormats._
import com.qwery.models._
import com.qwery.platform.codegen.spark.TableManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class OilGasSecurities() extends Serializable {
  @transient
  private val logger = LoggerFactory.getLogger(getClass)

  def start(args: Array[String])(implicit spark: SparkSession): Unit = {
     import spark.implicits._
     TableManager.add(Table(
  name = "Securities",
  columns = List(Column(name = "Symbol", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Name", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "LastSale", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "MarketCap", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "IPOyear", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Sector", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Industry", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "SummaryQuote", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Reserved", `type` = ColumnTypes.STRING, isNullable = true)),
  location = "./samples/companylist/csv/",
  fieldDelimiter = Some(","),
  fieldTerminator = None,
  headersIncluded = Some(true),
  nullValue = Some("n/a"),
  inputFormat = Some(StorageFormats.CSV),
  outputFormat = None,
  partitionColumns = List(),
  properties = Map(),
  serdeProperties = Map()
))
TableManager.add(Table(
  name = "OilGasSecurities",
  columns = List(Column(name = "Symbol", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Name", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "LastSale", `type` = ColumnTypes.DOUBLE, isNullable = true),Column(name = "MarketCap", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "IPOyear", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Sector", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Industry", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "SummaryQuote", `type` = ColumnTypes.STRING, isNullable = true),Column(name = "Reserved", `type` = ColumnTypes.STRING, isNullable = true)),
  location = "./temp/flink/companylist/csv/",
  fieldDelimiter = Some(","),
  fieldTerminator = None,
  headersIncluded = None,
  nullValue = None,
  inputFormat = None,
  outputFormat = Some(StorageFormats.CSV),
  partitionColumns = List(),
  properties = Map(),
  serdeProperties = Map()
))
TableManager.write(
   source = TableManager.read("Securities")
.where($"Industry" === lit("Oil/Gas Transmission"))
.select($"Symbol",$"Name",$"LastSale",$"MarketCap",$"IPOyear",$"Sector",$"Industry"),
   destination = TableManager("OilGasSecurities"),
   append = true
)
  }

}

object OilGasSecurities {
   private[this] val logger = LoggerFactory.getLogger(getClass)

   def main(args: Array[String]): Unit = {
     implicit val spark: SparkSession = createSparkSession("OilGasSecurities")
     new OilGasSecurities().start(args)
     spark.stop()
   }

   def createSparkSession(appName: String): SparkSession = {
     val sparkConf = new SparkConf()
     val builder = SparkSession.builder()
       .appName(appName)
       .config(sparkConf)
       .enableHiveSupport()

     // first attempt to create a clustered session
     try builder.getOrCreate() catch {
       // on failure, create a local one...
       case _: Throwable =>
         logger.warn(s"$appName failed to connect to EMR cluster; starting local session...")
         builder.master("local[*]").getOrCreate()
     }
   }
}

