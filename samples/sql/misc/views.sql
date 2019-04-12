begin

    create external table Securities (
            symbol string,
            name string,
            lastsale string,
            marketcap string,
            ipoyear string,
            sector string,
            industry string,
            summaryquote string,
            reserved string
        )
        row format delimited
        fields terminated by ','
        stored as inputformat 'CSV'
        with headers on
        with null values as 'n/a'
        location './samples/companylist/csv/';

    -- process the data
    create view  OilGasSecurities as
    select symbol, name, lastsale, marketcap, ipoyear, sector, industry
    from securities
    where industry = 'oil/gas transmission';

    show ( select * from OilGasSecurities );

end;