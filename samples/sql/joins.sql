----------------------------------------------------------------
--      Joins example
----------------------------------------------------------------
main program 'Joins' with batch processing {

    ----------------------------------------------------------------
    --      model definitions
    ----------------------------------------------------------------

    /* First, we define our input and output sources */
    log 'Describing the input and output sources... ';
    create table Securities (
            Symbol STRING, Name STRING, LastSale STRING, MarketCap STRING,
            IPOyear STRING, Sector STRING, Industry STRING, SummaryQuote STRING, Reserved STRING)
        row format delimited
        fields terminated by ','
        stored as inputformat 'CSV' outputformat 'CSV'
        location './samples/companylist/csv/';

    /* define the SelectSecurities table */
    create logical table SelectSecurities (Ticker STRING)
        from values ('AAPL'), ('ATO'), ('JE'), ('NFG'), ('LNG');

    ----------------------------------------------------------------
    --      transformations
    ----------------------------------------------------------------

    /* And finally, we perform our filtering/transformation */
    log 'Performing the transformation... ';
    set @dataSet = (
        select Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry, SummaryQuote, Reserved
        from Securities
        inner join SelectSecurities ON Ticker = Symbol
    );

    ----------------------------------------------------------------
    --      persistence
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
    show @dataSet limit 100;
};
