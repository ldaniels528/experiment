-----------------------------------------------------------------
--      Names Test
-----------------------------------------------------------------

create external table `lastnames`(`id` string, `lastname` string)
row format delimited
    fields terminated by ','
stored as INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
location
    's3://reporting-dev/raw_test/lastnames';

create external table `firstnames`(`id` string, `firstname` string)
row format delimited
    fields terminated by ','
stored as inputformat
    'org.apache.hadoop.mapred.TextInputFormat'
location
    's3://reporting-dev/raw_test/firstnames';

create external table `fullnames`(`id` string, `fullname` string)
row format delimited
    fields terminated by ','
stored as outputformat
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location
  's3://reporting-dev/target/fullnames';

select f.id, concat(f.firstname, ' ', l.lastname) as fullname
into table fullnames
from firstnames as f
left join lastnames as l on f.id = l.id;



