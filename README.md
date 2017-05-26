Qwery
===============
Qwery exposes a SQL-like query language to extract structured data from a file or REST URL. Qwery can be used as
an SDK or as a CLI application.

### Table of Contents

* <a href="#motivation">Motivation</a>
* <a href="#features">Features</a>
* <a href="#development">Build Requirements</a>
* <a href="#etl">Qwery ETL</a>
    * <a href="#how-it-works">How it works</a>
        * <a href="#sample-trigger-file">Sample Trigger Configuration file</a>
        * <a href="#sample-sql-script">Sample SQL script</a>
        * <a href="#file-archival">File Archival</a>
        * <a href="#building-etl">Building and running the ETL</a>
* <a href="#repl">Qwery REPL</a>
    * <a href="#repl_start">Start the REPL</a>
    * <a href="#describe">DESCRIBE the layout of a local file</a>
    * <a href="#describe_select">DESCRIBE a SELECT query</a>
* <a href="sdk">Qwery SDK</a>
* <a href="#faq">Frequently Asked Questions (FAQ)</a>

<a name="motivation"></a>
### Motivation

Systems like [Apache Storm](http://storm.apache.org) or [Spark Streaming](http://spark.apache.org) are powerful 
and flexible distributed processing engines, which are usually fed by a message-oriented middleware solution 
(e.g. [Apache Kafka](http://kafka.apache.org) or [Twitter Kestrel](https://github.com/twitter/kestrel)). 

The challenge that I've identified, is that organizations usually have to build a homegrown solution for the high-speed 
data/file ingestion into Kafka or Kestrel, which distracts them from their core focus. I've built Broadway to help provide 
a solution to that challenge.

<a name="features"></a>
### Features

Qwery provides the capability of invoking SQL-like queries against:
* Files (local, HTTP or S3)
* Avro-encoded or JSON-based Kafka topics 
* Database tables (Coming soon)

Additionally, Qwery has three modes of operation:
* ETL/Orchestration Server
* REPL/CLI tool
* Library/SDK

<a name="development"></a>
### Build Requirements

* [SBT v0.13.15](http://www.scala-sbt.org/download.html)

### Running the tests

```bash
$ sbt test
```

<a href="#etl"></a>
### Qwery ETL

<a name="how-it-works"></a>
#### How it works

Qwery uses a convention-over-configuration model. 

The root directory is defined using an environment variable QWERY_HOME. As long as the directory defined by this
variable exists, Qwery will create any necessary sub-directories.

The directory structure is as follows:

| Directory             | Purpose/Usage                                                 |
|-----------------------|---------------------------------------------------------------|
| $QWERY_HOME/archive   | The directory where Qwery stores processed files              |
| $QWERY_HOME/config    | The directory where Qwery looks for configuration files       |
| $QWERY_HOME/failed    | The directory where files that fail processing are moved to   |
| $QWERY_HOME/inbox     | The directory where Qwery looks for input files               |
| $QWERY_HOME/script    | The directory where Qwery looks for user-created SQL script files |
| $QWERY_HOME/work      | The directory where Qwery processes files                     |

The ETL module two things to create a workflow; a trigger configuration and a SQL script. The trigger configuration defines
which file(s) will be processed, and the SQL script describes how data will be extracted and where it will be written.

<a name="sample-trigger-file"></a>
##### Sample Trigger Configuration File

The following is an example of a simple trigger file configuration. This example essentially directs Qwery to look for 
files (in $QWERY_HOME/inbox) starting with "companylist" (prefix) and ending in ".csv" (suffix), and processing them 
using the script file.

```json
[{
    "name": "Company Lists",
    "constraints": [{ "prefix": "companylist" }, { "suffix": ".csv" }],
    "script": "companylist.sql"
}]
```

<a name="sample-sql-script"></a>
##### Sample SQL Script

The following is script to execute ($QWERY_HOME/scripts/companylist.sql) when the file has been observed:

```sql
INSERT INTO 'companylist.json' WITH JSON FORMAT (Symbol, Name, Sector, Industry)
SELECT Symbol, Name, Sector, Industry, `Summary Quote`
FROM 'companylist.csv' 
WITH CSV FORMAT
```

The above SQL script is simple enough, it reads CSV records from 'companylist.csv', and writes four of the fields in
JSON format to 'companylist.json'.

Additionally, this script works well if the input and output files are known ahead of time, but often this is not the case.
As a result, Qwery supports the substitution of pre-defined variables for the input file name. Consider the following
script which is functionally identical to the one above.

```sql
INSERT INTO '{{ work.file.base }}.json' WITH JSON FORMAT (Symbol, Name, Sector, Industry)
SELECT Symbol, Name, Sector, Industry, `Summary Quote`
FROM '{{ work.file.path }}'
WITH CSV FORMAT
```

The following are the variables that are created by the Workflow Manager at the time of processing:

| Variable name         | Purpose/Usage                                                         |
|-----------------------|-----------------------------------------------------------------------|
| work.file.base        | The base name of the input file being processed (e.g. "companylist")  |
| work.file.name        | The name of the input file being processed (e.g. "companylist.csv")   |
| work.file.path        | The full path of the input file being processed (e.g. "/full/path/to/companylist.csv")   |
| work.file.size        | The size (in bytes) of the input file                                 |
| work.path             | The full path of the processing sub-directory (underneath work)       |


The follow is a sample of the input file:

```csv
"Symbol","Name","LastSale","MarketCap","ADR TSO","IPOyear","Sector","Industry","Summary Quote",
"XXII","22nd Century Group, Inc","1.4","126977358.2","n/a","n/a","Consumer Non-Durables","Farming/Seeds/Milling","http://www.nasdaq.com/symbol/xxii",
"FAX","Aberdeen Asia-Pacific Income Fund Inc","5","1266332595","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/fax",
"IAF","Aberdeen Australia Equity Fund Inc","6.24","141912114.24","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/iaf",
"CH","Aberdeen Chile Fund, Inc.","7.06","66065291.4","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/ch",
"ABE           ","Aberdeen Emerging Markets Smaller Company Opportunities Fund I","13.63","131446834.05","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/abe",
"FCO","Aberdeen Global Income Fund, Inc.","8.62","75376107.36","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/fco",
"IF","Aberdeen Indonesia Fund, Inc.","7.4345","69173383.372","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/if",
"ISL","Aberdeen Israel Fund, Inc.","18.4242","73659933.1758","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/isl",
```

And here's an example of the output file:

```json
{"Sector":"Consumer Non-Durables","Name":"22nd Century Group, Inc","Industry":"Farming/Seeds/Milling","Symbol":"XXII","Summary Quote":"http://www.nasdaq.com/symbol/xxii"}
{"Sector":"n/a","Name":"Aberdeen Asia-Pacific Income Fund Inc","Industry":"n/a","Symbol":"FAX","Summary Quote":"http://www.nasdaq.com/symbol/fax"}
{"Sector":"n/a","Name":"Aberdeen Australia Equity Fund Inc","Industry":"n/a","Symbol":"IAF","Summary Quote":"http://www.nasdaq.com/symbol/iaf"}
{"Sector":"n/a","Name":"Aberdeen Chile Fund, Inc.","Industry":"n/a","Symbol":"CH","Summary Quote":"http://www.nasdaq.com/symbol/ch"}
{"Sector":"n/a","Name":"Aberdeen Emerging Markets Smaller Company Opportunities Fund I","Industry":"n/a","Symbol":"ABE","Summary Quote":"http://www.nasdaq.com/symbol/abe"}
{"Sector":"n/a","Name":"Aberdeen Global Income Fund, Inc.","Industry":"n/a","Symbol":"FCO","Summary Quote":"http://www.nasdaq.com/symbol/fco"}
{"Sector":"n/a","Name":"Aberdeen Indonesia Fund, Inc.","Industry":"n/a","Symbol":"IF","Summary Quote":"http://www.nasdaq.com/symbol/if"}
{"Sector":"n/a","Name":"Aberdeen Israel Fund, Inc.","Industry":"n/a","Symbol":"ISL","Summary Quote":"http://www.nasdaq.com/symbol/isl"}
{"Sector":"Capital Goods","Name":"Acme United Corporation.","Industry":"Industrial Machinery/Components","Symbol":"ACU","Summary Quote":"http://www.nasdaq.com/symbol/acu"}
{"Sector":"Consumer Services","Name":"ACRE Realty Investors, Inc.","Industry":"Real Estate Investment Trusts","Symbol":"AIII","Summary Quote":"http://www.nasdaq.com/symbol/aiii"}
```

<a name="file-archival"></a>
##### File Archival

By convention, on a file has been processed, Qwery stores the file in $QWERY_HOME/archive/_yyyy_/_mm_/_dd_/_hhmmss_/ where: 
* _yyyy_ is the 4-digit current year (e.g. 2017)
* _mm_ is the 2-digit current month (e.g. 05)
* _dd_ is the 2-digit current day of the month (e.g. 28)
* _hhmmss_ is the 6-digit current time (e.g. 061107)

<a name="building-etl"></a>
### Building and running the ETL

Building (and assembling) the ETL is simple:

```bash
~/Downloads/qwery/> sbt "project etl" clean assembly 
```

After the compilation completes, you'll see a message like:

```text
[info] SHA-1: 1be8ca09eefa6053fca04765813c01d134ed8d01
[info] SHA-1: e80143d4b7b945729d5121b8d87dbc7199d89cd4
[info] Packaging /Users/ldaniels/git/qwery/app/etl/target/scala-2.12/qwery-etl-0.3.0.bin.jar ...
[info] Packaging /Users/ldaniels/git/qwery/target/scala-2.12/qwery-core-assembly-0.3.0.jar ...
[info] Done packaging.
[info] Done packaging.
```

Now, you can execute the ETL distributable:

```bash
~/Downloads/qwery/> java -jar /Users/ldaniels/git/qwery/app/etl/target/scala-2.12/qwery-etl-0.3.0.bin.jar
```

*NOTE*: In order to run the ETL, you'll first have to define an environment variable (QWERY_HOME) telling the application 
where its "home" directory is. 

On a Mac, Linux or UNIX system:

```bash
~/Downloads/qwery/> export QWERY_HOME=./example
```

On a Windows system:

```bash
C:\Downloads\qwery\> set QWERY_HOME=.\example
```

Once it's up and running, it should look something like the following:

```text
[info] Running com.github.ldaniels528.qwery.etl.QweryETL 

 Qwery ETL v0.2.6
         ,,,,,
         (o o)
-----oOOo-(_)-oOOo-----
      
2017-05-28 19:56:10 INFO  ETLConfig:81 - Loading triggers from '/Users/ldaniels/git/qwery/example/config/triggers.json'...
2017-05-28 19:56:10 INFO  ETLConfig$:102 - [Company Lists] Compiling script 'companylist.sql'...
2017-05-28 19:56:10 INFO  QweryETL$:61 - Hello.
2017-05-28 19:56:51 INFO  QweryETL$:54 - [eebd64f1-5b95-458e-a2d3-745494718697] Company Lists ~> 'companylist.csv'
[INFO] [05/28/2017 19:56:51.533] [qwery-akka.actor.default-dispatcher-2] [akka://qwery/user/$b] [eebd64f1-5b95-458e-a2d3-745494718697] Preparing to process Inbox:'companylist.csv'
[INFO] [05/28/2017 19:56:51.706] [qwery-akka.actor.default-dispatcher-2] [akka://qwery/user/$b] [eebd64f1-5b95-458e-a2d3-745494718697] Process completed successfully in 173 msec
[INFO] [05/28/2017 19:56:51.711] [qwery-akka.actor.default-dispatcher-2] [akka://qwery/user/$b] [eebd64f1-5b95-458e-a2d3-745494718697] 359 records, 0 failures, 358 batch (4917.8 records/sec, 758.66 KB/sec)
[INFO] [05/28/2017 19:56:51.714] [qwery-akka.actor.default-dispatcher-4] [akka://qwery/user/$a] Moving 'eebd64f1-5b95-458e-a2d3-745494718697' to '/Users/ldaniels/git/qwery/example/archive/2017/05/28/075651/eebd64f1-5b95-458e-a2d3-745494718697'
```

<a name="repl"></a>
### Qwery REPL 

Qwery offers a command line interface (CLI), which allows interactive querying or files, REST endpoints, etc.

<a name="repl_start"></a>
##### Start the REPL

```text
ldaniels@Spartan:~$ sbt run

 Qwery CLI v0.3.0
         ,,,,,
         (o o)
-----oOOo-(_)-oOOo-----
You can executes multi-line queries here like:

SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap, `Summary Quote` FROM './companylist.csv'
WHERE Sector = 'Basic Industries'
LIMIT 5;
      
Using UNIXCommandPrompt for input.
```

<a name="describe"></a>
##### DESCRIBE the layout of a local file:

```text
[1]> DESCRIBE './companylist.csv';

+ --------------------------------------------------------------------------------------- +
| COLUMN         TYPE    SAMPLE                                                           |
+ --------------------------------------------------------------------------------------- +
| Symbol         String  ABE                                                              |
| Name           String  Aberdeen Emerging Markets Smaller Company Opportunities Fund I   |
| LastSale       String  13.63                                                            |
| MarketCap      String  131446834.05                                                     |
| ADR TSO        String  n/a                                                              |
| IPOyear        String  n/a                                                              |
| Sector         String  n/a                                                              |
| Industry       String  n/a                                                              |
| Summary Quote  String  http://www.nasdaq.com/symbol/abe                                 |
+ --------------------------------------------------------------------------------------- +
```

<a name="describe_select"></a>
##### DESCRIBE a SELECT query

```text
[2]> DESCRIBE (SELECT Symbol, Name, Sector, Industry,  CAST(LastSale AS DOUBLE) AS LastSale, CAST(MarketCap AS DOUBLE) AS MarketCap FROM 'companylist.csv');

+ -------------------------------------------- +
| COLUMN     TYPE    SAMPLE                    |
+ -------------------------------------------- +
| Symbol     String  XXII                      |
| Name       String  22nd Century Group, Inc   |
| Sector     String  Consumer Non-Durables     |
| Industry   String  Farming/Seeds/Milling     |
| LastSale   Double  1.4                       |
| MarketCap  Double  1.269773582E8             |
+ -------------------------------------------- +
```

##### Count the number of (non-blank) lines in the file:

```text
[3]> SELECT COUNT(*) FROM './companylist.csv';

+ ---------- +
| SYQWYsxb   |
+ ---------- +
| 359        |
+ ---------- +
```

##### Count the number of lines that match a given set of criteria in the file:

```text
[4]> SELECT COUNT(*) FROM './companylist.csv' WHERE Sector = 'Basic Industries';

+ ---------- +
| pUREGxhj   |
+ ---------- +
| 44         |
+ ---------- +
```

##### Sum values (just like you normally do with SQL) in the file:

```text
[5]> SELECT SUM(LastSale) AS total FROM './companylist.csv' LIMIT 5;

+ --------------- +
| total           |
+ --------------- +
| 77.1087         |
+ --------------- +
```

##### Type-casting and column name aliases are supported:

```text
[6]>  SELECT CAST('1234' AS Double) AS number

+ -------- + 
| number   | 
+ -------- + 
| 1234.0   | 
+ -------- + 
```

##### Select fields from the file using criteria:

```text
[7]> SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap FROM './companylist.csv' WHERE Industry = 'EDP Services';

+ -------------------------------------------------------------------------------- +
| Symbol  Name                   Sector      Industry      LastSale  MarketCap     |
+ -------------------------------------------------------------------------------- +
| TEUM    Pareteum Corporation   Technology  EDP Services  0.775     9893729.05    |
| WYY     WidePoint Corporation  Technology  EDP Services  0.44      36438301.68   |
+ -------------------------------------------------------------------------------- +
```

##### Aggregating data via GROUP BY

```text
[8]> SELECT Sector, COUNT(*) AS Securities FROM './companylist.csv' GROUP BY Sector

+ --------------------------------- + 
| Sector                 Securities | 
+ --------------------------------- + 
| Consumer Durables      4          | 
| Consumer Non-Durables  13         | 
| Energy                 30         | 
| Consumer Services      27         | 
| Transportation         1          | 
| n/a                    120        | 
| Health Care            48         | 
| Basic Industries       44         | 
| Public Utilities       11         | 
| Capital Goods          24         | 
| Finance                12         | 
| Technology             20         | 
| Miscellaneous          5          | 
+ --------------------------------- + 
```

##### CASE-WHEN is also supported

```text
[9]> SELECT
       CASE 'Hello World'
         WHEN 'HelloWorld' THEN 'Found 1'
         WHEN 'Hello' || ' ' || 'World' THEN 'Found 2'
         ELSE 'Not Found'
       END;

+ ---------- +
| JSOdavyb   |
+ ---------- +
| Found 2    |
+ ---------- +
```

##### Copy a portion of one file to another (appending the target)

```text
[10]> INSERT INTO './test2.csv' (Symbol, Sector, Industry, LastSale)
      SELECT Symbol, Sector, Industry, LastSale FROM './companylist.csv'
      WHERE Industry = 'Homebuilding'

+ --------------- +
| ROWS_INSERTED   |
+ --------------- +
| 1               |
+ --------------- +
```

##### Copy a portion of one file to another (overwriting the target)

```text
[11]> INSERT OVERWRITE './test2.csv' (Symbol, Sector, Industry, LastSale)
      SELECT Symbol, Sector, Industry, LastSale FROM './companylist.csv'
      WHERE Industry = 'Precious Metals'

+ --------------- +
| ROWS_INSERTED   |
+ --------------- +
| 44              |
+ --------------- +
```

<a name="sdk"></a>
### Qwery SDK

Qwery can also be used as a Library/SDK.

##### Let's start with a local file (./companylist.csv)

```csv
"Symbol","Name","LastSale","MarketCap","ADR TSO","IPOyear","Sector","Industry","Summary Quote",
"XXII","22nd Century Group, Inc","1.4","126977358.2","n/a","n/a","Consumer Non-Durables","Farming/Seeds/Milling","http://www.nasdaq.com/symbol/xxii",
"FAX","Aberdeen Asia-Pacific Income Fund Inc","5","1266332595","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/fax",
"IAF","Aberdeen Australia Equity Fund Inc","6.24","141912114.24","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/iaf",
"CH","Aberdeen Chile Fund, Inc.","7.06","66065291.4","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/ch",
"ABE           ","Aberdeen Emerging Markets Smaller Company Opportunities Fund I","13.63","131446834.05","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/abe",
"FCO","Aberdeen Global Income Fund, Inc.","8.62","75376107.36","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/fco",
"IF","Aberdeen Indonesia Fund, Inc.","7.4345","69173383.372","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/if",
"ISL","Aberdeen Israel Fund, Inc.","18.4242","73659933.1758","n/a","n/a","n/a","n/a","http://www.nasdaq.com/symbol/isl",
.
.
```

##### Let's examine the columns and values of the file

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the query
val query = QweryCompiler("DESCRIBE './companylist.csv'")
    
// execute the query    
val results = query.execute(RootScope()) // => Iterator[Seq[(String, Any)]]

// display the results as a table
new Tabular().transform(results) foreach println    
```

##### The Results

```text
+ --------------------------------------------------------------------------------------- + 
| COLUMN         TYPE    SAMPLE                                                           | 
+ --------------------------------------------------------------------------------------- + 
| Sector         String  n/a                                                              | 
| Name           String  Aberdeen Emerging Markets Smaller Company Opportunities Fund I   | 
| ADR TSO        String  n/a                                                              | 
| Industry       String  n/a                                                              | 
| Symbol         String  ABE                                                              | 
| IPOyear        String  n/a                                                              | 
| LastSale       String  13.63                                                            | 
| Summary Quote  String  http://www.nasdaq.com/symbol/abe                                 | 
| MarketCap      String  131446834.05                                                     | 
+ --------------------------------------------------------------------------------------- + 
```

##### Execute a Query against thr local file

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the query
val query = QweryCompiler(
  """
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Industry = 'Consumer Specialties'""".stripMargin)
    
// execute the query    
val results = query.execute(RootScope()) // => Iterator[Seq[(String, Any)]]

// display the results as a table
new Tabular().transform(results) foreach println
```

##### The Results

```text
+ ------------------------------------------------------------------------------------------------ + 
| Symbol  Name                  Sector             Industry              LastSale  MarketCap       | 
+ ------------------------------------------------------------------------------------------------ + 
| BGI     Birks Group Inc.      Consumer Services  Consumer Specialties  1.4401    25865464.7281   | 
| DGSE    DGSE Companies, Inc.  Consumer Services  Consumer Specialties  1.64      44125234.84     | 
+ ------------------------------------------------------------------------------------------------ + 
```

##### Or execute a Query against a REST-ful endpoint

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the query
val query = QweryCompiler(
  """
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap 
    |FROM 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
    |WHERE Sector = 'Oil/Gas Transmission'""".stripMargin)
    
// execute the query    
val results = query.execute(RootScope()) // => Iterator[Seq[(String, Any)]]

// display the results as a table
new Tabular().transform(results) foreach println
```

##### The Results

```text
+ -------------------------------------------------------------------------------------------------------------------- +
| Symbol  Name                                       Sector            Industry              LastSale  MarketCap       |
+ -------------------------------------------------------------------------------------------------------------------- +
| CQH     Cheniere Energy Partners LP Holdings, LLC  Public Utilities  Oil/Gas Transmission  25.68     5950056000      |
| CQP     Cheniere Energy Partners, LP               Public Utilities  Oil/Gas Transmission  31.75     10725987819     |
| LNG     Cheniere Energy, Inc.                      Public Utilities  Oil/Gas Transmission  45.35     10786934946.1   |
| EGAS    Gas Natural Inc.                           Public Utilities  Oil/Gas Transmission  12.5      131496600       |
+ -------------------------------------------------------------------------------------------------------------------- +
```

##### Copy (append) filtered results from one source (csv) to another (csv)

The source file (./companylist.csv) contains 360 lines of CSV text. The following query will filter these for 
records where the "Sector" field contains the text "Basic Industries", and write the results to the output file (./test1.csv)

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the statement
val statement = QweryCompiler(
  """
    |INSERT INTO './test1.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Sector = 'Basic Industries'""".stripMargin)

// execute the query
val results = statement.execute(RootScope())

// display the results as a table
new Tabular().transform(results) foreach println
```

##### Output

```text
+ --------------- +
| ROWS_INSERTED   |
+ --------------- +
| 44              |
+ --------------- +
```

##### Alternatively, you could overwrite the file instead of appending it...

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the statement
val statement = QweryCompiler(
  """
    |INSERT OVERWRITE './test1.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Sector = 'Basic Industries'""".stripMargin)

// execute the query
val results = statement.execute(RootScope())

// display the results as a table
new Tabular().transform(results) foreach println
```

##### Output

```text
+ --------------- +
| ROWS_INSERTED   |
+ --------------- +
| 44              |
+ --------------- +
```

##### And the output file (./test1.csv) will contain:

```text
"Symbol","Name","Sector","Industry","LastSale","MarketCap"
"AXU","Alexco Resource Corp","Basic Industries","Precious Metals","1.43","138634117.05"
"AAU","Almaden Minerals, Ltd.","Basic Industries","Precious Metals","1.47","132378409.8"
"USAS","Americas Silver Corporation","Basic Industries","Precious Metals","2.99","118908646.22"
"AKG","Asanko Gold Inc.","Basic Industries","Mining & Quarrying of Nonmetallic Minerals (No Fuels)","2.45","498032832.15"
"ASM","Avino Silver","Basic Industries","Precious Metals","1.52","79710321.52"
.
.
```

##### Copy filtered results from one source (csv) to another (json)

The source file (./companylist.csv) contains 360 lines of CSV text. The following query will filter these for 
records where the "Sector" field contains the text "Basic Industries", and write the results to the output file (./test1.csv)

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the statement
val statement = QweryCompiler(
  """
    |INSERT INTO './test1.json' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Sector = 'Basic Industries'""".stripMargin)

// execute the query
val results = statement.execute(RootScope())

// display the results as a table
new Tabular().transform(results) foreach println
```

##### Output

```text
+ --------------- +
| ROWS_INSERTED   |
+ --------------- +
| 44              |
+ --------------- +
```

##### And the output file (./test1.json) will contain:

```json
{"Sector":"Basic Industries","Name":"Alexco Resource Corp","Industry":"Precious Metals","Symbol":"AXU","LastSale":"1.43","MarketCap":"138634117.05"}
{"Sector":"Basic Industries","Name":"Almaden Minerals, Ltd.","Industry":"Precious Metals","Symbol":"AAU","LastSale":"1.47","MarketCap":"132378409.8"}
{"Sector":"Basic Industries","Name":"Americas Silver Corporation","Industry":"Precious Metals","Symbol":"USAS","LastSale":"2.99","MarketCap":"118908646.22"}
{"Sector":"Basic Industries","Name":"Asanko Gold Inc.","Industry":"Mining & Quarrying of Nonmetallic Minerals (No Fuels)","Symbol":"AKG","LastSale":"2.45","MarketCap":"498032832.15"}
{"Sector":"Basic Industries","Name":"Avino Silver","Industry":"Precious Metals","Symbol":"ASM","LastSale":"1.52","MarketCap":"79710321.52"}
.
.
```

<a name="faq"></a>
### Frequently Asked Questions (FAQ)

**Q**: How do I reference a field that contains spaces or special characters?

**A**: Use back ticks (\`). 

**Q**: Is ORDER BY supported?

**A**: No, ORDER BY is not yet supported.

**Q**: Is GROUP BY supported?

**A**: Yes; however, only for a single column

**Q**: Are VIEWs supported?

**A**: No. However, sub-queries can be used in place of views (e.g. SELECT name, date, age FROM (SELECT name, date, CAST(age AS DOUBLE) AS age))
