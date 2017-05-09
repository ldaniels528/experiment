Qwery
===============

#### Description

Qwery exposes a SQL-like query language to extract structured data from a file or REST URL. Qwery can be used as
an SDK or as a CLI application.

### Build Requirements

* [SBT v0.13.15](http://www.scala-sbt.org/download.html)

### Running the tests

```bash
$ sbt test
```

### Frequently Asked Questions (FAQ)

**Q**: How do I reference a field that contains spaces or special characters?

**A**: Use backticks (\`). 
```sql
SELECT `last name`, `first name`, position, startDate from './personnel.csv'
```

### Examples

##### The input file (./companylist.csv)

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

##### Execute a Query against a local file

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the query
val compiler = new QweryCompiler()
val query = compiler.compile(
  """
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Industry = 'Consumer Specialties'""".stripMargin)
    
// execute the query    
val scope = new RootScope()
val results = query.execute(scope) // => TraversableOnce[Seq[(String, Any)]]

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
val compiler = new QweryCompiler()
val query = compiler.compile(
  """
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap 
    |FROM 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
    |WHERE Sector = 'Oil/Gas Transmission'""".stripMargin)
    
// execute the query    
val scope = new RootScope()
val results = query.execute(scope) // => TraversableOnce[Seq[(String, Any)]]

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

##### Copy filtered results from one source (csv) to another (csv)

The source file (./companylist.csv) contains 360 lines of CSV text. The following query will filter these for 
records where the "Sector" field contains the text "Basic Industries", and write the results to the output file (./test1.csv)

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.qwery.ops._
import com.github.ldaniels528.tabular.Tabular

// compile the statement
val compiler = new QweryCompiler()
val statement = compiler.compile(
  """
    |INSERT INTO './test1.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Sector = 'Basic Industries'""".stripMargin)

// execute the query
val scope = new RootScope()
val results = statement.execute(scope)

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
val compiler = new QweryCompiler()
val statement = compiler.compile(
  """
    |INSERT INTO './test1.json' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Sector = 'Basic Industries'""".stripMargin)

// execute the query
val scope = new RootScope()
val results = statement.execute(scope)

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