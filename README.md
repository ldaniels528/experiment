Qwery
===============

#### Description

Qwery exposes a SQL-like query language to extract structured data from a file or REST URL.

### Build Requirements

* [SBT v0.13.15](http://www.scala-sbt.org/download.html)

### Running the tests

```bash
$ sbt test
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
import com.github.ldaniels528.tabular.Tabular

// compile the query
val compiler = new QueryCompiler()
val query = compiler.compile(
  """
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Industry = 'Oil/Gas Transmission'""".stripMargin)
    
// execute the query    
val results = query.execute() // => TraversableOnce[Seq[(String, Any)]]

// display the results as a table
new Tabular().transform(results) foreach println
```

##### Or execute a Query against a REST-ful endpoint

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.tabular.Tabular

// compile the query
val compiler = new QueryCompiler()
val query = compiler.compile(
  """
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap 
    |FROM 'http://www.nasdaq.com/screening/companies-by-industry.aspx?exchange=AMEX&render=download'
    |WHERE Sector = 'Oil/Gas Transmission'""".stripMargin)
    
// execute the query     
val results = query.execute() // => TraversableOnce[Seq[(String, Any)]]

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

##### Copy filtered results from one source to another

The source file (./companylist.csv) contains 360 lines of CSV text. The following query will filter these for 
records where the "Sector" field contains the text "Basic Industries", and write the results to the output file (./test1.csv)

```scala
import com.github.ldaniels528.qwery._
import com.github.ldaniels528.tabular.Tabular

// compile the statement
val compiler = new QueryCompiler()
val statement = compiler.compile(
  """
    |INSERT INTO './test1.csv' (Symbol, Name, Sector, Industry, LastSale, MarketCap)
    |SELECT Symbol, Name, Sector, Industry, LastSale, MarketCap
    |FROM './companylist.csv'
    |WHERE Sector = 'Basic Industries'""".stripMargin)

// execute the query
val results = statement.execute()

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
"BTG","B2Gold Corp","Basic Industries","Precious Metals","2.54","2471589235.12"
"BAA","BANRO CORPORATION","Basic Industries","Precious Metals","0.1215","133355740.608"
"LEU","Centrus Energy Corp.","Basic Industries","Mining & Quarrying of Nonmetallic Minerals (No Fuels)","5.19","46710000"
"LODE","Comstock Mining, Inc.","Basic Industries","Precious Metals","0.1825","34200513.6875"
"DNN","Denison Mine Corp","Basic Industries","Precious Metals","0.52","290716869.04"
"UUUU","Energy Fuels Inc","Basic Industries","Mining & Quarrying of Nonmetallic Minerals (No Fuels)","1.75","122921372"
"EGI","Entree Gold Inc","Basic Industries","Precious Metals","0.4603","79541205.7101"
"EMX","Eurasian Minerals Inc.","Basic Industries","Precious Metals","0.828","61408379.88"
"XRA","Exeter Resource Corporation","Basic Industries","Mining & Quarrying of Nonmetallic Minerals (No Fuels)","1.6601","149417720.5053"
"FSI","Flexible Solutions International Inc.","Basic Industries","Major Chemicals","1.56","17879145.96"
"GMO","General Moly, Inc","Basic Industries","Precious Metals","0.3675","33421643.64"
"GORO","Gold Resource Corporation","Basic Industries","Precious Metals","3.3","187571415.9"
"GSV","Gold Standard Ventures Corporation","Basic Industries","Mining & Quarrying of Nonmetallic Minerals (No Fuels)","1.7","378751795.8"
"AUMN","Golden Minerals Company","Basic Industries","Precious Metals","0.5399","48159901.1879"
"GSS","Golden Star Resources, Ltd","Basic Industries","Precious Metals","0.7186","263525279.44"
"GV","Goldfield Corporation (The)","Basic Industries","Water Supply","5.45","138709879.3"
"SIM","Grupo Simec, S.A. de C.V.","Basic Industries","Steel/Iron Ore","11.12","1844842149.52"
"THM","International Tower Hill Mines Ltd","Basic Industries","Precious Metals","0.5","81093486"
"KLDX","Klondex Mines Ltd.","Basic Industries","Precious Metals","3.59","636552090.4"
"MAG","MAG Silver Corporation","Basic Industries","Precious Metals","12.22","986836401.46"
"NSU","Nevsun Resources Ltd","Basic Industries","Precious Metals","2.27","685243976.11"
"NGD","New Gold Inc.","Basic Industries","Precious Metals","2.84","1634384400.6"
"NAK","Northern Dynasty Minerals, Ltd.","Basic Industries","Precious Metals","1.6","478060116.8"
"NG","Novagold Resources Inc.","Basic Industries","Precious Metals","4.18","1344542470.04"
"TIS","Orchids Paper Products Company","Basic Industries","Paper","24.35","250875395.85"
"PZG","Paramount Gold Nevada Corp.","Basic Industries","Precious Metals","1.6299","28979547.0246"
"PLG","Platinum Group Metals Ltd.","Basic Industries","Precious Metals","1.19","176662862.53"
"PLM","Polymet Mining Corp.","Basic Industries","Precious Metals","0.7098","226103609.3862"
"SAND          ","Sandstorm Gold Ltd","Basic Industries","Precious Metals","3.48","528940056.12"
"SKY","Skyline Corporation","Basic Industries","Homebuilding","6.22","52193537.68"
"XPL","Solitario Exploration & Royalty Corp","Basic Industries","Precious Metals","0.77","29787595.53"
"TRX","Tanzanian Royalty Exploration Corporation","Basic Industries","Precious Metals","0.51","60079485.12"
"TGB","Taseko Mines Limited","Basic Industries","Precious Metals","1.14","257807732.76"
"TGD","Timmons Gold Corp","Basic Industries","Precious Metals","0.395","140473297.79"
"TMQ","Trilogy Metals Inc.","Basic Industries","Precious Metals","0.705","74404165.92"
"URG","Ur Energy Inc","Basic Industries","Precious Metals","0.55","80230643.9"
"UEC","Uranium Energy Corp.","Basic Industries","Precious Metals","1.24","170763394.6"
"VGZ","Vista Gold Corporation","Basic Industries","Precious Metals","1.03","101142197.24"
"WRN","Western Copper and Gold Corporation","Basic Industries","Precious Metals","1.1233","106815273.2266"
```