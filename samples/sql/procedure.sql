----------------------------------------------------------------
--      companylist
----------------------------------------------------------------
main program 'companylist'
with batch processing
with hive support
{

    /* First, we define our input and output sources */
    log 'Loading the input and output sources...';
    include './samples/sql/companylist-input.sql';
    include './samples/sql/companylist-output-json.sql';

    ----------------------------------------------------------------
    --      functions and procedures
    ----------------------------------------------------------------
    --add jar '/home/taobao/oplog/hivescript/my_udf.jar';
    create function nullFix as 'com.github.ldaniels528.qwery.NullFix';

    create procedure testInserts(industry string) {
        /* here we perform our filtering/transformation */
        select Symbol, `Name`, LastSale, MarketCap, nullFix(IPOyear) as IPOyear, Sector, Industry
        from Securities
        where Industry = 'Oil/Gas Transmission' /* @industry */
    };

    ----------------------------------------------------------------
    --      the transformation
    ----------------------------------------------------------------
    log 'Performing the transformation...';
    set @dataSet = ( call testInserts('Oil/Gas Transmission') );
    insert overwrite table OilGasSecurities (Symbol, `Name`, LastSale, MarketCap, IPOyear, Sector, Industry)
    values @dataSet;

    -- show the first 5 rows
    show @dataSet limit 5;

};