----------------------------------------------------------------
--      Playing with Files example
----------------------------------------------------------------

begin

    -- first, let's define a logical table
    create inline table MyFiles (
        name string,
        absolutePath string,
        length string,
        canExecute boolean,
        canRead boolean,
        canWrite boolean,
        parent string,
        isDirectory boolean,
        isFile boolean,
        isHidden boolean
    )
    from filesystem('.');

    -- display the top 20 results
    show (
        select * from MyFiles
        where name like '%.csv'
        order by length desc
    ) limit 20;

end