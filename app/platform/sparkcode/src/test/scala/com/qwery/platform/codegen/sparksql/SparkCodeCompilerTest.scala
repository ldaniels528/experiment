package com.qwery.platform.codegen.sparksql

import com.qwery.language.SQLLanguageParser
import com.qwery.models.expressions.{Field, IsNotNull, IsNull}
import com.qwery.models.expressions.SQLFunction._
import com.qwery.platform.codegen.sparksql.SparkCodeCompiler._
import org.scalatest.FunSpec

/**
  * Spark Code Compiler Test
  * @author lawrence.daniels@gmail.com
  */
class SparkCodeCompilerTest extends FunSpec {

  import com.qwery.models.expressions.implicits._
  import com.qwery.util.StringHelper._

  describe(SparkCodeCompiler.getObjectSimpleName) {

    ////////////////////////////////////////////////////////////////////
    //        Equalities
    ////////////////////////////////////////////////////////////////////

    it("should compile conditions: age = 50") {
      assert((Field('age) === 50).compile == """$"age" === lit(50)""")
    }

    it("should compile conditions: age != 50") {
      assert((Field('age) !== 50).compile == """$"age" =!= lit(50)""")
    }

    it("should compile conditions: age < 50") {
      assert((Field('age) < 50).compile == """$"age" < lit(50)""")
    }

    it("should compile conditions: age <= 50") {
      assert((Field('age) <= 50).compile == """$"age" <= lit(50)""")
    }

    it("should compile conditions: age > 50") {
      assert((Field('age) > 50).compile == """$"age" > lit(50)""")
    }

    it("should compile conditions: age >= 50") {
      assert((Field('age) >= 50).compile == """$"age" >= lit(50)""")
    }

    it("should compile conditions: content is not null") {
      assert(IsNotNull('content).compile == """$"content".isNotNull""")
    }

    it("should compile conditions: content is null") {
      assert(IsNull('content).compile == """$"content".isNull""")
    }

    ////////////////////////////////////////////////////////////////////
    //        Mathematics
    ////////////////////////////////////////////////////////////////////


    ////////////////////////////////////////////////////////////////////
    //        SQL Functions
    ////////////////////////////////////////////////////////////////////

    it("should compile conditions: abs(cost) < 5000") {
      assert((Abs('cost) < 5000).compile == """abs($"cost") < lit(5000)""")
    }

    it("should compile conditions: date_add(cost, taxes)") {
      assert(Date_Add('cost, 'taxes).compile == """date_add($"cost", $"taxes")""")
    }

    it("should compile conditions: floor(cost)") {
      assert(Floor('cost).compile == """floor($"cost")""")
    }

    it("should compile conditions: if(cost > 5000, 'Yes', 'No')") {
      assert(If(Field('cost) > 5000, "Yes", "No").compile == """when($"cost" > lit(5000), lit("Yes")).otherwise(lit("No"))""")
    }

    it("should compile conditions: ltrim(description)") {
      assert(LTrim('description).compile == """ltrim($"description")""")
    }

    it("should compile conditions: max(age) < 50") {
      assert((Max('age) < 50).compile == """max($"age") < lit(50)""")
    }

    it("should compile conditions: min(age) > 10") {
      assert((Min('age) > 10).compile == """min($"age") > lit(10)""")
    }

    it("should compile conditions: rtrim(description)") {
      assert(RTrim('description).compile == """rtrim($"description")""")
    }

    it("should compile conditions: substring(title, 1, 4) = 'Hello'") {
      assert((Substring('title, 1, 4) === "Hello").compile == """substring($"title", 1, 4) === lit("Hello")""")
    }

    it("should compile conditions: sum(cost) < 5000") {
      assert((Sum('cost) < 5000).compile == """sum($"cost") < lit(5000)""")
    }

    it("should compile conditions: year(purchaseDate) = 2019") {
      assert((Year('purchaseDate) === 2019).compile == """year($"purchaseDate") === lit(2019)""")
    }

    ////////////////////////////////////////////////////////////////////
    //        Introspection
    ////////////////////////////////////////////////////////////////////

    it("should identify all input sources from an invokable") {
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
           |from (
           |   select 'DFP' as source
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
           |) as ab
           |on dfp.client_id = ab.client_id
           |group by dfp.client_id, ab.client_id;
           |""".stripMargin
      )
      val tables = SparkCodeCompiler.discoverTablesAndViews(model)
      info(s"tables: $tables")
      assert(tables == List("kbb_lkp_dfp_o1_advertiser", "kbb_ab_client"))
    }

  }

}
