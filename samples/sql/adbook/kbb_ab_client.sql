begin

    create external table kbb_ab_client(
        client_type String,
        client_id String,
        client_name String,
        client_address String,
        client_city String,
        client_state String,
        client_postal String,
        client_country String,
        url String,
        client_since String,
        customer_code String,
        client_currency String,
        adserver_mapping_id String,
        client_external_id String,
        default_price_type String,
        default_billing_model String,
        default_rate_card String,
        truncate_decimals_on_total_cost String,
        io_po_number_required String,
        client_crm_id String,
        credit_risk String,
        credit_limit String,
        prepay_required String,
        client_note String,
        client_attribute_group String,
        client_attribute_name String,
        account_manager String,
        primary_account_manager String,
        contact_id String,
        contact_first_name String,
        contact_last_name String,
        job_title String,
        contact_address1 String,
        contact_address2 String,
        contact_city String,
        contact_state String,
        contact_postal String,
        contact_country String,
        contact_phone String,
        contact_cell String,
        contact_fax String,
        email String,
        contact_code String,
        location_code String,
        crm_id String,
        contact_external_id String,
        billing String,
        materials String
    )
    row format delimited
    stored as inputformat 'CSV'
    with headers on
    with tblproperties ('skip.header.line.count'='1', 'transient_lastDdlTime'='1548444883')
    with serdeproperties ('quoteChar'='\"', 'separatorChar'=',')
    --location 's3://reference-kbb-raw-dev/adbook/client';
    location './temp/reference-kbb-raw-dev/adbook/client/';

end;