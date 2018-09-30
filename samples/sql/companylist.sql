----------------------------------------------------------------
--      companylist
----------------------------------------------------------------
main program 'companylist'
    with arguments as @args
    with environment as @env
    with batch processing
as
begin

    /* First, we define our input and output sources */

    info 'Loading the input and output sources... ';
    include './samples/sql/companylist-input.sql';
    include './samples/sql/companylist-output-json.sql';

    /* And finally, we perform our filtering/transformation */

    insert overwrite table OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry, SummaryQuote, Reserved)
    select Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry, SummaryQuote, Reserved
    from Securities
    where Industry = 'Oil/Gas Transmission';

    /* Show some data */

    show (
        select Symbol, Name, Sector, Industry, SummaryQuote
        from Securities
        where Industry = 'Oil/Gas Transmission'
    ) limit 5;

end
;
