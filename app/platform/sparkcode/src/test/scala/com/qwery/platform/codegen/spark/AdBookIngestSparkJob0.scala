package com.qwery.platform.codegen.spark

import com.qwery.models._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class AdBookIngestSparkJob0() extends Serializable {
  @transient
  private val logger = LoggerFactory.getLogger(getClass)

  def start(args: Array[String])(implicit spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    TableManager.add(Table(
      name = "kbb_ab_client",
      columns = List(Column(name = "client_type", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_name", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_address", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_city", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_state", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_postal", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_country", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "url", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_since", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "customer_code", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_currency", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "adserver_mapping_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_external_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "default_price_type", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "default_billing_model", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "default_rate_card", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "truncate_decimals_on_total_cost", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "io_po_number_required", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_crm_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "credit_risk", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "credit_limit", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "prepay_required", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_note", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_attribute_group", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "client_attribute_name", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "account_manager", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "primary_account_manager", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_first_name", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_last_name", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "job_title", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_address1", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_address2", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_city", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_state", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_postal", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_country", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_phone", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_cell", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_fax", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "email", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_code", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "location_code", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "crm_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "contact_external_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "billing", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "materials", `type` = ColumnTypes.STRING, isNullable = true)),
      location = "./temp/AdBook/Client/",
      fieldDelimiter = None,
      fieldTerminator = None,
      headersIncluded = Some(true),
      nullValue = None,
      inputFormat = Some(StorageFormats.CSV),
      outputFormat = None,
      partitionColumns = List(),
      properties = Map("skip.header.line.count" -> "1", "transient_lastDdlTime" -> "1548444883"),
      serdeProperties = Map("quoteChar" -> "\"", "separatorChar" -> ",")
    ))
    TableManager.add(Table(
      name = "kbb_lkp_dfp_o1_advertiser",
      columns = List(Column(name = "source", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "advertiser_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "parent_advertiser_id", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "advertiser_name", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "advertiser_type", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "brand", `type` = ColumnTypes.STRING, isNullable = true), Column(name = "src_created_ts_est", `type` = ColumnTypes.TIMESTAMP, isNullable = true), Column(name = "src_modified_ts_est", `type` = ColumnTypes.TIMESTAMP, isNullable = true), Column(name = "last_processed_ts_est", `type` = ColumnTypes.TIMESTAMP, isNullable = true)),
      location = "./temp/kbb_lkp_dfp_o1_advertiser/",
      fieldDelimiter = None,
      fieldTerminator = None,
      headersIncluded = None,
      nullValue = None,
      inputFormat = Some(StorageFormats.PARQUET),
      outputFormat = None,
      partitionColumns = List(),
      properties = Map(),
      serdeProperties = Map()
    ))

    val df_kbb_ab_client = TableManager.read("kbb_ab_client")
      .filter(lower($"client_type") === lit("client"))
      .groupBy($"client_id", $"client_type", $"client_name", $"client_since")
      .agg(
        max(when(lower($"client_attribute_group") === lit("revenue category"), $"client_attribute_name")).as("revenue_category"),
        max(when(lower($"client_attribute_group") === lit("brand"), $"client_attribute_name")).as("brand")
      )
      .select(
        lit("AB").as("source")
        , $"client_id"
        , lit(-1).as("agency_id")
        , $"client_name"
        , $"revenue_category"
        , $"brand"
      ).as("ab")

    val df_kbb_lkp_dfp_o1_advertiser = TableManager.read("kbb_lkp_dfp_o1_advertiser")
      .select(
        lit("DFP").as("source")
        , $"advertiser_id".as("client_id")
        , lit(-1).as("agency_id")
        , $"advertiser_name".as("client_name")
        , when(substring($"advertiser_name", 1, 6) === lit("Dealer"), lit("T3 - Individual Dealer"))
          .when(substring($"advertiser_name", 1, 2) === lit("T3"), lit("T3 - Individual Dealer"))
          .when(substring($"advertiser_name", 1, 6) === lit("ATC T3"), lit("T3 - Individual Dealer"))
          .when(substring($"advertiser_name", 1, 2) === lit("T2"), lit("T2 - Dealer Assoc"))
          .when(substring($"advertiser_name", 1, 4) === lit("Nend"), lit("Nend"))
          .when($"advertiser_name" like "%Unsold %", lit("Remnant"))
          .when($"advertiser_name" like "%Remnant %", lit("Remnant"))
          .when($"advertiser_name" like "%OEM%", lit("T1 - OEM"))
          .when(substring($"advertiser_name", 1, 2) === lit("T1"), lit("T1 - OEM"))
          .when($"advertiser_name" like "%Partner%", lit("Partner"))
          .when($"advertiser_name" like "%KBB AdX%", lit("Ad Exchange"))
          .when($"advertiser_name" like "%KBB%", lit("House"))
          .otherwise("").as("revenue_category")
        , lit("").as("brand")
        , lit("").as("src_created_ts_est")
      )
      .where($"advertiser_id".isNotNull)

    val df_1553041415153 = df_kbb_lkp_dfp_o1_advertiser.as("dfp")
      .join(df_kbb_ab_client.as("ab"), $"dfp.client_id" === $"ab.client_id", "full_outer")
      .groupBy($"dfp.client_id", $"ab.client_id")
      .agg(
        max(coalesce($"ab.source", $"dfp.source")).as("source")
        , coalesce($"ab.client_id", $"dfp.client_id").as("client_id")
        , max($"ab.agency_id").as("agency_id")
        , max(coalesce($"ab.client_name", $"dfp.client_name")).as("client_name")
        , max(coalesce($"ab.revenue_category", $"dfp.revenue_category")).as("revenue_category")
        , max($"ab.brand").as("brand")
      )
      .select($"source", $"ab.client_id", $"agency_id", $"client_name", $"revenue_category", $"brand")

    // TODO manually added for verification purposes
    df_kbb_ab_client.show(5)
    df_kbb_lkp_dfp_o1_advertiser.show(5)
    df_1553041415153.show(5)
  }

}

object AdBookIngestSparkJob0 {
  private[this] val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = createSparkSession("AdBookIngestSparkJob")
    new AdBookIngestSparkJob0().start(args)
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

