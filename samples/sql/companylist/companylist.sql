----------------------------------------------------------------
--      companylist
----------------------------------------------------------------
begin

    /* First, we define our input and output sources */

    info 'Loading the input and output sources... ';
    include './samples/sql/companylist/companylist-input.sql';
    include './samples/sql/companylist/companylist-output-json.sql';

    /* Show some data */

    show (
        select Symbol, Name, Sector, Industry, SummaryQuote
        from Securities
        where Industry = 'Oil/Gas Transmission'
    ) limit 5;

end
