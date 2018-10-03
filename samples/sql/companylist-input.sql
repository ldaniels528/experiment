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
    with headers on
    with null values as 'n/a'
    location './samples/companylist/csv/';

end
;