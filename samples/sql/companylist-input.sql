begin

    /* First, we define our input source */

    create external table Securities (
        Symbol STRING,
        Name STRING,
        LastSale STRING,
        MarketCap STRING,
        IPOyear STRING,
        Sector STRING,
        Industry STRING,
        SummaryQuote STRING,
        Reserved STRING
    )
    row format delimited
    fields terminated by ','
    stored as inputformat 'CSV'
    outputformat 'CSV'
    location './samples/companylist/csv/';

end
;