package com.qwery.examples.nasdaq.companylist;

------------------------------------------------------------
-- the companylist raw file
------------------------------------------------------------

create external table companylist (
    Symbol string,
    Name string,
    LastSale double,
    MarketCap double,
    IPOyear int,
    Sector string,
    Industry string,
    `Summary Quote` string,
    Dummy string
)
row format delimited
fields terminated by ','
stored as textfile
location "./app/runtime/src/test/resources/companylist/";

------------------------------------------------------------
-- Our Stocks table
------------------------------------------------------------

create table Stocks (
    Symbol string,
    Name string,
    LastSale double,
    MarketCap double,
    IPOyear int,
    Sector string,
    Industry string
)
row format delimited
fields terminated by ','
stored as textfile
location "./temp/A";

------------------------------------------------------------
-- Our Sectors table
------------------------------------------------------------

create table Sectors (
    Sector string,
    Industry string,
    DeptCode string,
    AvgPrice double,
    Companies int
)
row format delimited
fields terminated by ','
stored as textfile
location "./temp/B";

------------------------------------------------------------
-- the Transformations
------------------------------------------------------------

insert into Stocks (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
select Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
from companylist
order by Symbol
limit 100

insert into Sectors (Sector, Industry, DeptCode, AvgPrice, Companies)
select Sector, Industry, '1337' AS DeptCode, Avg(LastSale), COUNT(*) AS Companies
from companylist
order by Sector

