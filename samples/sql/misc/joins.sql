----------------------------------------------------------------
--      Joins example
----------------------------------------------------------------
begin

    ----------------------------------------------------------------
    --      model definitions
    ----------------------------------------------------------------

    /* First, we define our input and output sources */
    info 'Describing the input and output sources... ';
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

    /* define the SelectSecurities table */
    create inline table SelectSecurities (Ticker STRING)
        from values ('AAPL'), ('ATO'), ('JE'), ('NFG'), ('LNG');

    ----------------------------------------------------------------
    --      transformations
    ----------------------------------------------------------------

    /**
     *  show the data
     *  +------+--------------------+--------+---------+-------+----------------+--------------------+--------------------+--------+------+
     *  |Symbol|                Name|LastSale|MarketCap|IPOyear|          Sector|            Industry|        SummaryQuote|Reserved|Ticker|
     *  +------+--------------------+--------+---------+-------+----------------+--------------------+--------------------+--------+------+
     *  |   ATO|Atmos Energy Corp...|   92.12|  $10.24B|    n/a|Public Utilities|Oil/Gas Transmission|https://www.nasda...|    null|   ATO|
     *  |    JE|Just Energy Group...|     2.8| $417.27M|    n/a|Public Utilities|Oil/Gas Transmission|https://www.nasda...|    null|    JE|
     *  |   NFG|National Fuel Gas...|   56.48|   $4.85B|    n/a|Public Utilities|Oil/Gas Transmission|https://www.nasda...|    null|   NFG|
     *  |   LNG|Cheniere Energy, ...|   62.76|  $15.57B|    n/a|Public Utilities|Oil/Gas Transmission|https://www.nasda...|    null|   LNG|
     *  +------+--------------------+--------+---------+-------+----------------+--------------------+--------------------+--------+------+
     */

    /* And finally, we perform our filtering/transformation */
    info 'Performing the transformation... ';
    show (
        select Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry, SummaryQuote, Reserved
        from Securities
        inner join SelectSecurities ON Ticker = Symbol
    ) limit 100;

end;
