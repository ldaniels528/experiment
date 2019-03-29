package com.qwery.platform.sparksql.generator

import com.qwery.language.SQLLanguageParser
import SparkInlineSQLCompilerTest._
import org.scalatest.FunSpec

import scala.language.postfixOps

/**
  * SQL Decompiler Test
  * @author lawrence.daniels@gmail.com
  */
class SparkInlineSQLCompilerTest extends FunSpec {

  import SparkInlineSQLCompiler.Implicits._
  import com.qwery.util.StringHelper._

  describe(SparkInlineSQLCompiler.getObjectSimpleName) {
    implicit val appArgs: ApplicationArgs = ApplicationArgs()

    it("should translate a model into SQL") {
      val decompiledSQL = SQLLanguageParser.parse(
        """|select max(coalesce(ab.source, dfp.source)) as source
           |            ,coalesce(ab.client_id, dfp.client_id) as client_id
           |            ,max(ab.agency_id) as agency_id
           |            ,max(coalesce(ab.client_name, dfp.client_name)) as client_name
           |            ,max(coalesce(ab.revenue_category, dfp.revenue_category)) as revenue_category
           |            ,max(ab.brand) as brand
           |    from (
           |         select 'DFP' as source
           |            , advertiser_id as client_id
           |            , -1 as agency_id
           |            , advertiser_name as client_name
           |            ,(case
           |                  when substr(advertiser_name, 1, 6) = 'Dealer' then 'T3 - Individual Dealer'
           |                  when substr(advertiser_name, 1, 2) = 'T3' then 'T3 - Individual Dealer'
           |                  when substr(advertiser_name, 1, 6) = 'ATC T3' then 'T3 - Individual Dealer'
           |                  when substr(advertiser_name, 1, 2) = 'T2' then 'T2 - Dealer Assoc'
           |                  when substr(advertiser_name, 1, 4) = 'Nend' then 'Nend'
           |                  when advertiser_name like '%Unsold %' then 'Remnant'
           |                  when advertiser_name like '%Remnant %' then 'Remnant'
           |                  when advertiser_name like '%OEM%' then 'T1 - OEM'
           |                  when substr(advertiser_name, 1, 2) = 'T1' then 'T1 - OEM'
           |                  when advertiser_name like '%Partner%' then'Partner'
           |                  when advertiser_name like '%KBB AdX%' then'Ad Exchange'
           |                  when advertiser_name like '%KBB%' then 'House'
           |                  else ''
           |             end) as revenue_category
           |            ,'' as brand
           |            ,'' as src_created_ts_est
           |         from kbb_lkp_dfp_o1_advertiser
           |         where advertiser_id is not null
           |    ) as dfp
           |    full join (
           |        select 'AB' as source
           |            ,client_id as client_id
           |            , -1 as agency_id
           |            , client_name
           |            ,max(case when lower(client_attribute_group) = 'revenue category' then client_attribute_name end) as revenue_category
           |            ,max(case when lower(client_attribute_group) = 'brand' then client_attribute_name end) as brand
           |        from kbb_ab_client
           |        where lower(client_type) = 'client'
           |        group by client_id,client_type,client_name,client_since
           |    ) as ab
           |    on dfp.client_id = ab.client_id
           |    group by dfp.client_id, ab.client_id
           |""".stripMargin
      ) toSQL

      info("\nDecompiled SQL:")
      info(decompiledSQL)

      assert(decompiledSQL isSameAs
        """|SELECT
           |  MAX(COALESCE(ab.source,dfp.source)) AS source,
           |  COALESCE(ab.client_id,dfp.client_id) AS client_id,
           |  MAX(ab.agency_id) AS agency_id,
           |  MAX(COALESCE(ab.client_name,dfp.client_name)) AS client_name,
           |  MAX(COALESCE(ab.revenue_category,dfp.revenue_category)) AS revenue_category,
           |  MAX(ab.brand) AS brand
           |FROM (
           |  SELECT
           |    'DFP' AS source,
           |    advertiser_id AS client_id,
           |    -1.0 AS agency_id,
           |    advertiser_name AS client_name,
           |    CASE WHEN SUBSTR(advertiser_name, 1, 6) = 'Dealer' THEN 'T3 - Individual Dealer'
           |      WHEN SUBSTR(advertiser_name, 1, 2) = 'T3' THEN 'T3 - Individual Dealer'
           |      WHEN SUBSTR(advertiser_name, 1, 6) = 'ATC T3' THEN 'T3 - Individual Dealer'
           |      WHEN SUBSTR(advertiser_name, 1, 2) = 'T2' THEN 'T2 - Dealer Assoc'
           |      WHEN SUBSTR(advertiser_name, 1, 4) = 'Nend' THEN 'Nend'
           |      WHEN advertiser_name like '%Unsold %' THEN 'Remnant'
           |      WHEN advertiser_name like '%Remnant %' THEN 'Remnant'
           |      WHEN advertiser_name like '%OEM%' THEN 'T1 - OEM'
           |      WHEN SUBSTR(advertiser_name, 1, 2) = 'T1' THEN 'T1 - OEM'
           |      WHEN advertiser_name like '%Partner%' THEN 'Partner'
           |      WHEN advertiser_name like '%KBB AdX%' THEN 'Ad Exchange'
           |      WHEN advertiser_name like '%KBB%' THEN 'House'
           |      ELSE ''
           |    END AS revenue_category,
           |    '' AS brand,
           |    '' AS src_created_ts_est
           |  FROM global_temp.kbb_lkp_dfp_o1_advertiser
           |  WHERE advertiser_id IS NOT NULL
           |) AS dfp
           |FULL OUTER JOIN (
           |  SELECT
           |    'AB' AS source,
           |    client_id AS client_id,
           |    -1.0 AS agency_id,
           |    client_name,
           |    MAX(CASE WHEN LOWER(client_attribute_group) = 'revenue category' THEN client_attribute_name END) AS revenue_category,
           |    MAX(CASE WHEN LOWER(client_attribute_group) = 'brand' THEN client_attribute_name END) AS brand
           |  FROM  global_temp.kbb_ab_client
           |  WHERE LOWER(client_type) = 'client'
           |  GROUP BY client_id,client_type,client_name,client_since
           |) AS ab
           |ON dfp.client_id = ab.client_id
           |GROUP BY dfp.client_id,ab.client_id
           |""".stripMargin
      )
    }

  }

}

/**
  * SQL Decompiler Test Companion
  * @author lawrence.daniels@gmail.com
  */
object SparkInlineSQLCompilerTest {
  private val invalids = " \t\n\r".toCharArray

  /**
    * SQL Clean-up Extensions
    * @param lines the given concatenation of lines of SQL text
    */
  final implicit class SQLCleanupExtensions(val lines: String) extends AnyVal {

    @inline def isSameAs(text: String): Boolean = normalize(lines) equalsIgnoreCase normalize(text)

    @inline def normalize(text: String): String = text.filterNot(invalids.contains(_))

  }

}