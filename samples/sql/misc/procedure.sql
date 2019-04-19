----------------------------------------------------------------
--      Procedures Example
----------------------------------------------------------------
begin

    -- First, we define our input and output sources
    info 'Loading the input and output sources...';
    include './samples/sql/companylist/companylist-input.sql';
    include './samples/sql/companylist/companylist-output-json.sql';

    ----------------------------------------------------------------
    --      functions and procedures
    ----------------------------------------------------------------

    --add jar './lib/my_udf.jar';

    create function nullFix as 'com.github.ldaniels528.qwery.NullFix';

    create procedure lookupIndustry(industry string) as
    begin
        -- here we perform our filtering/transformation
        select Symbol, `Name`, LastSale, MarketCap, coalesce(nullFix(IPOyear), '') as IPOyear, Sector, Industry
        from Securities
        where Industry = '$industry'
    end;

    ----------------------------------------------------------------
    --      the transformation
    ----------------------------------------------------------------

    info 'Performing the transformation...';
    set @dataSet = ( call lookupIndustry('Oil/Gas Transmission') );

    insert overwrite table OilGasSecurities (Symbol, `Name`, LastSale, MarketCap, IPOyear, Sector, Industry)
    values @dataSet;

    -- show the first 5 rows
    show @dataSet limit 20;

end;