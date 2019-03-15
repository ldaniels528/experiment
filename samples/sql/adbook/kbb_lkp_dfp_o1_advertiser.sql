begin

    create external table kbb_lkp_dfp_o1_advertiser(
        source String,
        advertiser_id String,
        parent_advertiser_id String,
        advertiser_name String,
        advertiser_type String,
        brand String,
        src_created_ts_est Timestamp,
        src_modified_ts_est Timestamp,
        last_processed_ts_est Timestamp
    )
    stored as inputformat parquet
    location 's3://kbb-etl-edp/data/parquet/kbb_lkp_dfp_o1_advertiser/';

end;