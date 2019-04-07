----------------------------------------------------------------
--      LiveRamp Ingest
----------------------------------------------------------------
begin

    /**
      * LiveRamp input format
      * {
      *   "profile_id":"463f14dd1ab7a18d830cf6fe7f5ee9429bb84fc2",
      *   "visitor_id":"03ee25c536ee663685997f31e1f3ad0d4830457e94744a1bf3752a57a5d407194873a8ac4d227d8d",
      *   "identity_type":"Digital",
      *   "liveramp_ingestion_date":1531115764000,
      *   "__index_level_0__":"0",
      *   "year":2018,
      *   "month":7,
      *   "day":9
      * }
      */
    create table LiveRamp (
        profile_id STRING,
        visitor_id STRING,
        identity_type STRING,
        liveramp_ingestion_date LONG,
        __index_level_0__ STRING,
        year INTEGER,
        month INTEGER,
        day INTEGER
    )
    stored as inputformat 'PARQUET'
    location './temp/awsdhubnp-consumer-ma-exporter-pixall/';

    /**
      * LiveRamp output format
      * {
      *   "profile_id":"463f14dd1ab7a18d830cf6fe7f5ee9429bb84fc2",
      *   "visitor_id":"03ee25c536ee663685997f31e1f3ad0d4830457e94744a1bf3752a57a5d407194873a8ac4d227d8d",
      *   "identity_type":"Digital",
      *   "liveramp_ingestion_date":1531115764000
      * }
      */
    create table LiveRampCSV (
        profile_id STRING,
        visitor_id STRING,
        identity_type STRING,
        liveramp_ingestion_date LONG
    )
    row format delimited
    fields terminated by ','
    stored as outputformat 'CSV'
    location './temp/flink/liveramp/csv/';

    ----------------------------------------------------------------
    -- transform the data
    ----------------------------------------------------------------
    info 'Transforming the input data...';
    insert overwrite table LiveRampCSV (profile_id, visitor_id, identity_type, liveramp_ingestion_date)
    select profile_id, visitor_id, identity_type, liveramp_ingestion_date
    from LiveRamp;

    show (
        select profile_id, visitor_id, identity_type, liveramp_ingestion_date from LiveRamp
    ) limit 5;

end
;