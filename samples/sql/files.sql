----------------------------------------------------------------
--      Playing with Files example
----------------------------------------------------------------
main program 'Playing with Files' with batch processing {

    set @files = (
        select * from (filesystem('/Users/ldaniels3/Downloads'))
        where name like '%.csv'
        order by length desc
    );

    show @files;
}