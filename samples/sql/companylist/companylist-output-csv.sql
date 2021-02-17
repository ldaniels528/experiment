begin

    /* Next, we define our output source */

    create external table OilGasSecurities (
        Symbol STRING(20),
        Name STRING(20),
        LastSale DOUBLE,
        MarketCap STRING(20),
        IPOyear STRING(20),
        Sector STRING(20),
        Industry STRING(20),
        SummaryQuote STRING(20),
        Reserved STRING(20)
    )
    /*row format delimited
    fields terminated by ','*/
    stored as outputformat 'CSV'
    location './temp/out/companylist/csv/';

end
;