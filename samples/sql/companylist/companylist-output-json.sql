begin

    /* Next, we define our output source */

    create table OilGasSecurities (
        Symbol STRING,
        Name STRING,
        LastSale DOUBLE,
        MarketCap STRING,
        IPOyear STRING,
        Sector STRING,
        Industry STRING,
        SummaryQuote STRING,
        Reserved STRING
    )
    /*row format delimited
    fields terminated by ','*/
    stored as outputformat 'JSON'
    location './temp/out/companylist/json/';

end
;