SELECT ROW_NUMBER() OVER (
        PARTITION BY hit_dt_est
        ,site_experience_desc ORDER BY tot_avg_rev DESC
            ,section
            ,super_section
            ,page_name
        ) AS rank
    ,section
    ,super_section
    ,page_name
    ,CAST(tot_avg_rev AS DECIMAL(10, 5)) AS rev
    ,hit_dt_est
    ,site_experience_desc
FROM (
--  since the final output has contains one unique page per day per site_experience, internal_rank is created as a way
--  to keep only the highest revenue generating page in the final output.
--  For a given date, and site_experience, internal_rank for any ss-s-p is caculated
--  by taking the average revenue by page regardless of ss-s and multiply
--  by the page view count by ss-s-p.  The page that generates the highest revenue
--  or internal_rank = 1 is kept in the final output.
--  ss-s-p = super section, section, page
    SELECT G.section AS section
        ,G.super_section AS super_section
        ,G.page_name AS page_name
        ,G.rev AS rev
        ,G.tot_avg_rev AS tot_avg_rev
        ,ROW_NUMBER() OVER (
            PARTITION BY hit_dt_est
            ,page_name
            ,site_experience_desc ORDER BY G.rev * G.pageview_cnt DESC
            ) AS internal_rank
        ,G.hit_dt_est AS hit_dt_est
        ,G.site_experience_desc AS site_experience_desc
    FROM (
        SELECT F.section AS section
            ,F.super_section AS super_section
            ,F.page_name AS page_name
            ,avg(F.rev) AS rev
            ,F.tot_avg_rev AS tot_avg_rev
            ,count(*) AS pageview_cnt  -- changed from number_of_visits
            ,F.hit_dt_est AS hit_dt_est
            ,F.site_experience_desc AS site_experience_desc
        FROM (
            SELECT D.section AS section
                ,D.super_section AS super_section
                ,D.page_name AS page_name
                ,D.rev AS rev             -- avg rev at ss-s-p
                ,avg(D.rev) OVER (        -- avg rev at page only
                    PARTITION BY hit_dt_est
                    ,page_name
                    ,site_experience_desc
                    ) AS tot_avg_rev
                ,D.hit_dt_est AS hit_dt_est
                ,D.site_experience_desc AS site_experience_desc
            FROM (
                SELECT A.hit_dt_est
                    ,A.section
                    ,A.super_section
                    ,A.page_name
                    ,A.hit_id
                    ,A.site_experience_desc
                    ,c.capped_dt
                    ,sum(CASE
                            WHEN C.lineitem_pricing_quantity = '1'
                                THEN 0.0
                            WHEN C.lineitem_pricing_type = 'CPM'
                                AND C.lineitem_package_group_type = 'PlaceHolder'
                                THEN 0.0
                            WHEN A.page_name NOT LIKE '%_zip'
                              AND A.hit_dt_est <= C.capped_dt
                                AND C.lineitem_pricing_type = 'CPM'
                                AND A.ad_product_offering_group NOT IN (
                                    '08_New Car Tablet'
                                    ,'09_Misc. Flat Fee'
                                    )
                                AND C.order_primary_team_name IN (
                                    'Biz Dev/Reseller'
                                    ,'OEM'
                                    )
                                AND C.advertiser_type NOT LIKE ('T3*')
                                THEN CAST(C.lineitem_pricing_rate AS DECIMAL(10, 4)) * (A.ad_impression_count / 1000.0)
                            ELSE 0.0
                            END) AS rev
                FROM (
                    SELECT hit_id
--                      ,visit_id
--                      ,visitor_id
                        ,hit_dt_est
                        ,regexp_replace(section,"\\<style\\>.*\\<\\/style\\>|\\<script src=","") as section
                        ,regexp_replace(super_section,"\\<style\\>.*\\<\\/style\\>|\\<script src=","") as super_section
                        ,regexp_replace(page_name,"\\<style\\>.*\\<\\/style\\>|\\<script src=","") as page_name
                        ,lineitem_id dfp_lineitem_id
                        --,li.oms_lineitem_id
                        ,ODA.dfp_ad_product_id AS dfp_ad_product_id
                        ,NAP.ad_product_offering_group
                        ,CASE
                            WHEN lower(site_experience_desc) LIKE '%desktop%'
                                THEN 'desktop'
                            WHEN lower(site_experience_desc) LIKE '%mobile%'
                                THEN 'mobile'
                            ELSE 'unknown'
                            END AS site_experience_desc
                        ,impression_count AS ad_impression_count
                        ,1 AS join_field
                    FROM $vwODA ODA
                    INNER JOIN (
                    SELECT ad_product_id, ad_product_offering_group FROM $vwAdproduct
                     UNION ALL
                     SELECT position_id as ad_product_id, 'na' ad_product_offering_group FROM(
                      SELECT distinct position_id FROM $vwAdookDrop) A
                     )NAP ON ODA.dfp_ad_product_id = NAP.ad_product_id AND page_name NOT LIKE 'svy%'
                    --LEFT JOIN kbb_edp.kbb_lkp_dfp_o1_lineitem li ON ODA.lineitem_id = li.dfp_lineitem_id
                    ) A
                INNER JOIN (
                    ( --Adbook
                    SELECT
                         drops.ad_server_drop_id dfp_lineitem_id
                        --,drops.drop_id oms_lineitem_id
                        ,capped.capped_dt capped_dt
                        ,drops.impressions_sold lineitem_pricing_quantity
                        ,drops.price_type lineitem_pricing_type
                        ,'1' lineitem_package_group_type
                        ,'OEM' order_primary_team_name
                        ,client.revenue_category advertiser_type
                        ,drops.price lineitem_pricing_rate
                    FROM (
                        SELECT *
                        FROM $vwAdookDrop
                        WHERE ad_server_drop_id != '-1'
                        ) drops
                  LEFT JOIN $vwLineitemCapped capped ON drops.ad_server_drop_id = capped.dfp_lineitem_id
                    LEFT JOIN $vwAdbookCampaign campaign ON drops.campaign_id = campaign.campaign_id
                    LEFT JOIN $vwAdbookClient client ON drops.client_id = client.client_id
                    )
                    union
                    ( -- O1
                    SELECT
                         lisrc.dfp_lineitem_id
                      --  ,lisrc.oms_lineitem_id
                        ,capped.capped_dt capped_dt
                        ,lisrc.lineitem_pricing_quantity
                        ,lisrc.lineitem_pricing_type
                        ,lisrc.lineitem_package_group_type
                        ,ordr.order_primary_team_name
                        ,adv.advertiser_type
                        ,lisrc.lineitem_pricing_rate
                    FROM (
                        SELECT *
                        FROM $vwLineitem
                        WHERE dfp_lineitem_id != '-1'
                        ) lisrc
                    LEFT JOIN $vwLineitemCapped capped ON lisrc.dfp_lineitem_id = capped.dfp_lineitem_id
                    LEFT JOIN $vwOrder ordr ON lisrc.order_id = ordr.order_id
                    LEFT JOIN $vwAdvertiser adv ON lisrc.advertiser_id = adv.advertiser_id
                    )
                    ) C ON A.dfp_lineitem_id = C.dfp_lineitem_id
                GROUP BY A.hit_dt_est
                    ,A.section
                    ,A.super_section
                    ,A.page_name
                    ,A.hit_id
                    ,A.site_experience_desc
                    ,c.capped_dt
                ) D
            WHERE (page_name NOT LIKE '%_zip')
                OR (
                    page_name LIKE '%_zip'
                    AND rev > 0
                    )
            ) F
        GROUP BY F.section
            ,F.super_section
            ,F.page_name
            ,F.hit_dt_est
            ,F.site_experience_desc
            ,F.tot_avg_rev -- grouping on page level
        ) G
    ) H
WHERE internal_rank = 1