begin

    /* First, we define our input source */

    create external table Securities (
        Symbol STRING(20),
        Name STRING(20),
        LastSale STRING(20),
        MarketCap STRING(20),
        IPOyear STRING(20),
        Sector STRING(20),
        Industry STRING(20),
        SummaryQuote STRING(20),
        Reserved STRING(20)
    )
    row format delimited
    fields terminated by ','
    stored as inputformat 'CSV'
    with headers on
    with null values as 'n/a'
    location './samples/companylist/csv/';

end
;