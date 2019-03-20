package com.qwery.platform.codegen.spark

import com.qwery.language.SQLLanguageParser
import org.scalatest.FunSpec

/**
  * AdBook Ingestion Test
  * @author lawrence.daniels@gmail.com
  */
class AdBookIngestTest extends FunSpec {

  describe(classOf[SQLLanguageParser].getSimpleName) {

    it("should generate the AdBookIngest Spark job") {
      val model = SQLLanguageParser.parse(
        """|include './samples/sql/adbook/kbb_ab_client.sql';
           |include './samples/sql/adbook/kbb_lkp_dfp_o1_advertiser.sql';
           |
           |select max(coalesce(ab.source, dfp.source)) as source
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
           |         from kbb_lkp_dfp_o1_advertiser as src
           |         where src.advertiser_id is not null
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
           |    group by dfp.client_id, ab.client_id;
           |""".stripMargin
      )
      val generator = new SparkCodeGenerator(
        className = "AdBookIngestSparkJob",
        packageName = "com.qwery.platform.codegen.spark",
        outputPath = "./app/platform/sparkcode/src/test/scala")
      generator.generate(model)
    }

  }

}
