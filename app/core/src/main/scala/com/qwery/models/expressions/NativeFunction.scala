package com.qwery.models.expressions

/**
  * Represents a native SQL function with any number of parameters
  * @author lawrence.daniels@gmail.com
  */
case class NativeFunction(name: String,
                          minArgs: Int,
                          maxArgs: Int,
                          description: String,
                          usage: String,
                          override val isAggregate: Boolean) extends NamedExpression

/**
  * Native Function
  * @author lawrence.daniels@gmail.com
  */
object NativeFunction {

  /**
    * Generates a function definition with zero parameters
    * @param name        the name of the function
    * @param description the description of the function
    * @param usage       an example of the function's usage
    * @param isAggregate indicates whether the function is an aggregate function
    * @return a zero-parameter function
    */
  def zero(name: String,
           description: String,
           usage: String = "",
           isAggregate: Boolean = false): NativeFunction =
    many(
      name = name,
      minArgs = 0,
      maxArgs = 0,
      description = description,
      usage = usage,
      isAggregate = isAggregate
    )

  /**
    * Generates a function definition with one parameter
    * @param name        the name of the function
    * @param description the description of the function
    * @param usage       an example of the function's usage
    * @param isAggregate indicates whether the function is an aggregate function
    * @return a single-parameter function
    */
  def one(name: String,
          description: String,
          usage: String = "",
          isAggregate: Boolean = false): NativeFunction =
    many(
      name = name,
      //minArgs = 1,
      maxArgs = 1,
      description = description,
      usage = usage,
      isAggregate = isAggregate
    )

  /**
    * Generates a function definition with two parameters
    * @param name        the name of the function
    * @param description the description of the function
    * @param usage       an example of the function's usage
    * @param isAggregate indicates whether the function is an aggregate function
    * @return a two-parameter function
    */
  def two(name: String,
          description: String,
          usage: String = "",
          isAggregate: Boolean = false): NativeFunction =
    many(
      name = name,
      minArgs = 2,
      maxArgs = 2,
      description = description,
      usage = usage,
      isAggregate = isAggregate
    )

  /**
    * Generates a function definition with three parameters
    * @param name        the name of the function
    * @param description the description of the function
    * @param usage       an example of the function's usage
    * @param isAggregate indicates whether the function is an aggregate function
    * @return a three-parameter function
    */
  def three(name: String,
            description: String,
            usage: String = "",
            isAggregate: Boolean = false): NativeFunction =
    many(
      name = name,
      minArgs = 3,
      maxArgs = 3,
      description = description,
      usage = usage,
      isAggregate = isAggregate
    )

  /**
    * Generates a function definition with variable parameters
    * @param name        the name of the function
    * @param minArgs     the minimum number of arguments to expect
    * @param maxArgs     the maximum number of arguments to expect
    * @param description the description of the function
    * @param usage       an example of the function's usage
    * @param isAggregate indicates whether the function is an aggregate function
    * @return a variable-parameter function
    */
  def many(name: String,
           minArgs: Int = 1,
           maxArgs: Int = Int.MaxValue,
           description: String,
           usage: String = "",
           isAggregate: Boolean = false): NativeFunction = {
    val fx = new NativeFunction(name, minArgs, maxArgs, description, usage = usage, isAggregate = isAggregate)
    fx.copy(usage = makeUsageExample(fx))
  }

  /**
    * Indicates whether the given name corresponds to a native function
    * @param name the given name
    * @return true, if the given name corresponds to a native function
    */
  def isNativeFunction(name: String): Boolean =
    nativeFunctions.exists { case (fxName, _) => fxName equalsIgnoreCase name }

  private def makeUsageExample(fx: NativeFunction): String = {
    val name = fx.name
    if (fx.usage.nonEmpty) fx.usage else {
      fx.maxArgs match {
        case 0 => s"$name()"
        case 1 => s"$name(expr)"
        case Int.MaxValue => s"$name(expr1, ...)"
        case maxArgs => s"$name(${(1 to maxArgs).map(n => s"expr$n").mkString(", ")})"
      }
    }
  }

  /**
    * The Native SQL functions
    * @see [[https://spark.apache.org/docs/2.4.0/api/sql/index.html]]
    * @see [[https://www.ibm.com/support/knowledgecenter/en/SSULQD_7.1.0/com.ibm.nz.dbu.doc/r_dbuser_functions.html]]
    */
  val nativeFunctions: Map[String, NativeFunction] = Map(Seq(
    one(name = "abs", description = "Returns the absolute value of the numeric value"),
    one(name = "acos", description =
      "Returns the inverse cosine (a.k.a. arc cosine) of expr, as if computed by [[java.lang.Math#acos]]"),
    two(name = "add_months", description =
      "Returns the date that is num_months after start_date", usage = "add_months(start_date, num_months)"),
    many(name = "aggregate", description =
      """|Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state.
         |The final state is converted into the final result by applying a finish function""".stripMargin),
    many(name = "approx_count_distinct", description =
      "Returns the estimated cardinality by HyperLogLog++. relativeSD defines the maximum estimation error allowed"),
    many(name = "approx_percentile", description =
      """|Returns the approximate percentile value of numeric column col at the given percentage.
         |The value of percentage must be between 0.0 and 1.0. The  accuracy parameter (default: 10000)
         |is a positive numeric literal which controls approximation accuracy at the cost of memory.
         |Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the approximation.
         |When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
         |In this case, returns the approximate percentile array of column col at the given percentage array""".stripMargin),
    many(name = "array", description =
      "Returns an array with the given elements"),
    two(name = "array_contains", description =
      "Returns true if the array contains the value"),
    many(name = "array_distinct", description = "Removes duplicate values from the array"),
    two(name = "array_except", description =
      "Returns an array of the elements in array1 but not in array2, without duplicates"),
    two(name = "array_index", description =
      "Returns an array of the elements in the intersection of array1 and array2, without duplicates"),
    two(name = "array_intersect",
      description = "Returns an array of the elements in the intersection of array1 and array2, without duplicates",
      usage = "array_intersect(array1, array2)"),
    many(name = "array_join", usage = "array_join(array, delimiter[, nullReplacement])", description =
      """|Concatenates the elements of the given array using the delimiter and an optional string to
         |replace nulls. If no value is set for nullReplacement, any null value is filtered""".stripMargin),
    one(name = "array_max", usage = "array_max(array)", description = "Returns the maximum value in the array. NULL elements are skipped"),
    one(name = "array_min", usage = "array_min(array)", description = "Returns the minimum value in the array. NULL elements are skipped"),
    two(name = "array_overlap", usage = "arrays_overlap(array1, array2)", description =
      """|Returns true if a1 contains at least a non-null element present also in a2. If the arrays have no
         |common element and they are both non-empty and either of them contains a null element null is returned,
         |false otherwise""".stripMargin),
    two(name = "array_position", usage = "array_position(array, element)",
      description = "Returns the (1-based) index of the first element of the array as long"),
    two(name = "array_remove", usage = "array_remove(array, element)",
      description = "Remove all elements that equal to element from array"),
    two(name = "array_repeat", usage = "array_repeat(element, count)",
      description = "Returns the array containing element count times"),
    one(name = "array_sort", usage = "array_sort(array)", description =
      """|Sorts the input array in ascending order. The elements of the input array must be orderable.
         |Null elements will be placed at the end of the returned array""".stripMargin),
    two(name = "array_union", usage = "array_union(array1, array2)",
      description = "Returns an array of the elements in the union of array1 and array2, without duplicates"),
    many(name = "arrays_zip", usage = "arrays_zip(array1, array2, ...)",
      description = "Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays"),
    one(name = "ascii", description = "Returns the numeric value of the first character of string"),
    one(name = "asin", description = "Returns the inverse sine (a.k.a. arc sine) the arc sin of expr, as if computed by [[java.lang.Math#asin]]"),
    one(name = "assert_true", description = "Throws an exception if expr is not true"),
    one(name = "atan", description = "Returns the inverse tangent (a.k.a. arc tangent) of expr, as if computed by [[java.lang.Math#atan]]"),
    two(name = "atan2", description =
      """|Returns the angle in radians between the positive x-axis of a plane and the point given by the
         |coordinates (exprX, exprY), as if computed by [[java.lang.Math#atan2]]""".stripMargin),
    one(name = "avg", description = "Returns the mean calculated from values of a group"),
    one(name = "base64", description = "Converts the argument from a binary bin to a base 64 string"),
    one(name = "bigint", description = "Casts the value expr to the target data type bigint"),
    one(name = "bin", description = "Returns the string representation of the long value expr represented in binary"),
    one(name = "binary", description = "Casts the value expr to the target data type binary"),
    one(name = "bit_length", description = "Returns the bit length of string data or number of bits of binary data"),
    one(name = "boolean", description = "Casts the value expr to the target data type boolean"),
    two(name = "bround", usage = "bround(expr, d)", description = "Returns expr rounded to d decimal places using HALF_EVEN rounding mode"),
    one(name = "cardinality", description =
      """|Returns the size of an array or a map. The function returns -1 if its input is null and
         |spark.sql.legacy.sizeOfNull is set to true. If spark.sql.legacy.sizeOfNull is set to false,
         |the function returns null for null input. By default, the spark.sql.legacy.sizeOfNull parameter
         |is set to true""".stripMargin),
    one(name = "cbrt", description = "Returns the cube root of expr"),
    one(name = "ceil", description = "Returns the smallest integer not smaller than expr"),
    one(name = "ceiling", description = "Returns the smallest integer not smaller than expr"),
    one(name = "char", description =
      """|Returns the ASCII character having the binary equivalent to expr.
         |If n is larger than 256 the result is equivalent to chr(n % 256)""".stripMargin),
    one(name = "char_length", description =
      """|Returns the character length of string data or number of bytes of binary data. The length of
         |string data includes the trailing spaces. The length of binary data includes binary zeros""".stripMargin),
    one(name = "character_length",
      description = "Collects and returns a list of non-unique elements"),
    one(name = "chr", description =
      """|Returns the ASCII character having the binary equivalent to expr.
         |If n is larger than 256 the result is equivalent to chr(n % 256)""".stripMargin),
    many(name = "coalesce", description = "Returns the first non-null argument if exists. Otherwise, null"),
    one(name = "collect_list", description = "Collects and returns a list of non-unique elements"),
    one(name = "collect_set", description = "Collects and returns a set of unique elements"),
    many(name = "concat", usage = "concat(col1, col2, ..., colN)", description = "Returns the concatenation of col1, col2, ..., colN"),
    many(name = "concat_ws", minArgs = 2, usage = "concat_ws(sep, [str | array(str)]+)",
      description = "Returns the concatenation of the strings separated by sep"),
    many(name = "conv", usage = "conv(num, from_base, to_base)",
      description = "Convert num from from_base to to_base"),
    two(name = "corr",
      description = "Returns Pearson coefficient of correlation between a set of number pairs"),
    one(name = "cos",
      description = "Returns the cosine of expr, as if computed by [[java.lang.Math#cos]]"),
    one(name = "cosh",
      description = "Returns the hyperbolic cosine of expr, as if computed by [[java.lang.Math#cosh]]"),
    one(name = "cot",
      description = "Returns the cotangent of expr, as if computed by [[java.lang.Math#cot]]"),
    many(name = "count",
      description = "Returns the total number of retrieved rows, including rows containing null"),
    many(name = "count_min_sketch", usage = "count_min_sketch(col, eps, confidence, seed)", description =
      """|Returns a count-min sketch of a column with the given esp, confidence and seed.
         |The result is an array of bytes, which can be deserialized to a CountMinSketch before usage.
         |Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space""".stripMargin),
    two(name = "covar_pop", description = "Returns the population covariance of a set of number pairs"),
    two(name = "covar_samp", description = "Returns the sample covariance of a set of number pairs"),
    one(name = "crc32",
      description = "Returns a cyclic redundancy check value of the expr as a bigint"),
    many(name = "cube",
      description = "[[https://spark.apache.org/docs/2.4.0/api/sql/index.html#cube]]"),
    zero(name = "cume_dist",
      description = "Computes the position of a value relative to all values in the partition"),
    zero(name = "current_database",
      description = "Returns the current database"),
    zero(name = "current_date",
      description = "Returns the current date at the start of query evaluation"),
    zero(name = "current_timestamp",
      description = "Returns the current timestamp at the start of query evaluation"),
    one(name = "date",
      description = "Casts the value expr to the target data type date"),
    two(name = "date_add", usage = "date_add(start_date, num_days)",
      description = "Returns the date that is num_days after start_date"),
    two(name = "date_format", usage = "date_format(timestamp, fmt)",
      description = "Converts timestamp to a value of string in the format specified by the date format fmt"),
    two(name = "date_sub", usage = "date_sub(start_date, num_days)",
      description = "Returns the date that is num_days before start_date"),
    two(name = "date_trunc", description =
      """|Returns timestamp ts truncated to the unit specified by the format model fmt.
         |fmt should be one of ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD",
         |"HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"]""".stripMargin),
    two(name = "datediff", usage = "datediff(endDate, startDate)",
      description = "Returns the number of days from startDate to endDate"),
    one(name = "day", usage = "day(date)",
      description = "Returns the day of month of the date/timestamp"),
    one(name = "dayofmonth", usage = "dayofmonth(date)",
      description = "Returns the day of month of the date/timestamp"),
    one(name = "dayofweek", usage = "dayofweek(date)",
      description = "Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday)"),
    one(name = "decimal",
      description = "Casts the value expr to the target data type decimal"),
    two(name = "decode", usage = "decode(bin, charset)",
      description = "Decodes the first argument using the second argument character set"),
    one(name = "degrees",
      description = "Converts radians to degrees"),
    zero(name = "dense_rank", description =
      """|Computes the rank of a value in a group of values. The result is one plus the previously assigned rank value.
         |Unlike the function rank, dense_rank will not produce gaps in the ranking sequence""".stripMargin),
    one(name = "distinct",
      description = ""),
    one(name = "double",
      description = "Casts the value expr to the target data type double"),
    zero(name = "e", description = "Returns Euler's number, e"),
    many(name = "elt", usage = "elt(n, input1, input2, ...)",
      description = "Returns the n-th input, e.g., returns input2 when n is 2"),
    two(name = "encode", usage = "encode(str, charset)",
      description = "Encodes the first argument using the second argument character set"),
    one(name = "exp",
      description = "Returns e to the power of expr"),
    one(name = "explode", description =
      "Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns"),
    one(name = "explode_outer", description =
      "Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns"),
    one(name = "expm1", description = "Returns exp(expr) - 1"),
    one(name = "factorial", description = "Returns the factorial of expr. expr is [0..20]. Otherwise, null"),
    many(name = "find_in_set", maxArgs = 2, description =
      """|Returns the index (1-based) of the given string (str) in the comma-delimited list (str_array).
         |Returns 0, if the string was not found or if the given string (str) contains a comma""".stripMargin),
    many(name = "first", maxArgs = 2, usage = "first(expr[, isIgnoreNull])",
      description = "Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values"),
    many(name = "first_value", maxArgs = 2, usage = "first_value(expr[, isIgnoreNull])", description =
      "Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values"),
    one(name = "flatten", usage = "flatten(arrayOfArrays)", description = "Transforms an array of arrays into a single array"),
    one(name = "float", description = "Casts the value expr to the target data type float"),
    one(name = "floor", description = "Returns the largest integer not greater than expr"),
    two(name = "format_number", description =
      """|Formats the number expr1 like '#,###,###.##', rounded to expr2 decimal places.
         |If expr2 is 0, the result has no decimal point or fractional part. expr2 also accept a user specified format.
         |This is supposed to function like MySQL's FORMAT""".stripMargin),
    many(name = "format_string", minArgs = 2, usage = "format_string(strfmt, obj, ...)", description =
      "Returns a formatted string from printf-style format strings"),
    many(name = "from_json", minArgs = 2, usage = "from_json(jsonStr, schema[, options])", description =
      "Returns a struct value with the given jsonStr and schema"),
    two(name = "from_unixtime", usage = "from_unixtime(unix_time, format)", description =
      "Returns unix_time in the specified format"),
    two(name = "from_utc_timestamp", usage = "from_utc_timestamp(timestamp, timezone)", description =
      """|Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC,
         |and renders that time as a timestamp in the given time zone.
         |For example, 'GMT+1' would yield '2017-07-14 03:40:00.0'""".stripMargin),
    two(name = "get_json_object", usage = "get_json_object(json_txt, path)", description = "Extracts a json object from path"),
    many(name = "greatest", description = "Returns the greatest value of all parameters, skipping null values"),
    many(name = "hash", description = "Returns a hash value of the arguments"),
    one(name = "hex", description = "Converts expr to hexadecimal"),
    one(name = "hour", usage = "hour(timestamp)", description = "Returns the hour component of the string/timestamp"),
    two(name = "hypot", description = "Returns sqrt(expr1^2 + expr2^2)"),
    many(name = "in", description = "Returns true if expr equals to any valN"),
    one(name = "initcap", usage = "initcap(str)", description =
      """|Returns str with the first letter of each word in uppercase. All other letters are in lowercase.
         |Words are delimited by white space""".stripMargin),
    one(name = "inline", description = "Explodes an array of structs into a table"),
    one(name = "inline_outer", description = "Explodes an array of structs into a table"),
    zero(name = "input_file_block_length", description = "Returns the length of the block being read, or -1 if not available"),
    zero(name = "input_file_block_start", description = "Returns the start offset of the block being read, or -1 if not available"),
    zero(name = "input_file_name", description = "Returns the name of the file being read, or empty string if not available"),
    two(name = "instr", usage = "instr(str, substr)", description =
      "Returns the (1-based) index of the first occurrence of substr in str"),
    one(name = "int", description = "Casts the value expr to the target data type int"),
    one(name = "isnan", description = "Returns true if expr is NaN, or false otherwise"),
    one(name = "isnotnull", description = "Returns true if expr is not null, or false otherwise"),
    one(name = "isnull", description = "Returns true if expr is null, or false otherwise"),
    many(name = "java_method", minArgs = 2, usage = "java_method(class, method[, arg1[, arg2 ..]])", description =
      "Calls a method with reflection"),
    many(name = "json_tuple", usage = "json_tuple(jsonStr, p1, p2, ..., pn)", description =
      """|Returns a tuple like the function get_json_object, but it takes multiple names.
         |All the input parameters and output column types are string""".stripMargin),
    one(name = "kurtosis", description = "Returns the kurtosis value calculated from values of a group"),
    many(name = "lag", usage = "lag(input[, offset[, default]])", description =
      """|Returns the value of input at the offsetth row before the current row in the window. The default value of
         |offset is 1 and the default value of default is null. If the value of  input at the offsetth row is null,
         |null is returned. If there is no such offset row (e.g., when the offset is 1, the first row of the window
         |does not have any previous row), default is returned""".stripMargin),
    many(name = "last", maxArgs = 2, usage = "last(expr[, isIgnoreNull])", description =
      """|Returns the last value of expr for a group of rows. If isIgnoreNull is true,
         |returns only non-null values""".stripMargin),
    one(name = "last_day", usage = "last_day(date)", description = "Returns the last day of the month which the date belongs to"),
    many(name = "last_value", maxArgs = 2, usage = "last_value(expr[, isIgnoreNull])", description =
      """|Returns the last value of expr for a group of rows. If isIgnoreNull is true,
         |returns only non-null values""".stripMargin),
    one(name = "lcase", usage = "lcase(str)", description = "Returns str with all characters changed to lowercase"),
    many(name = "lead", maxArgs = 3, usage = "lead(input[, offset[, default]]) ", description =
      """|Returns the value of input at the offsetth row after the current row in the window. The default value of
         |offset is 1 and the default value of default is null. If the value of  input at the offsetth row is null,
         |null is returned. If there is no such an offset row (e.g., when the offset is 1, the last row of the window
         |does not have any subsequent row), default is returned""".stripMargin),
    many(name = "least", description = "Returns the least value of all parameters, skipping null values"),
    two(name = "left", usage = "left(str, len)", description =
      """|Returns the leftmost len(len can be string type) characters from the string str,
         |if len is less or equal than 0 the result is an empty string""".stripMargin),
    one(name = "length", description =
      """|Returns the character length of string data or number of bytes of binary data. The length of string data
         |includes the trailing spaces. The length of binary data includes binary zeros""".stripMargin),
    two(name = "levenshtein", usage = "levenshtein(str1, str2)", description =
      "Returns the Levenshtein distance between the two given strings"),
    one(name = "ln", description = "Returns the natural logarithm (base e) of `expr`"),
    many(name = "locate", minArgs = 2, maxArgs = 3, usage = "locate(substr, str[, pos])", description =
      """|Returns the position of the first occurrence of substr in str after position pos.
         |The given pos and return value are 1-based""".stripMargin),
    two(name = "log", usage = "log(base, expr)", description = "Returns the logarithm of `expr` with `base`"),
    one(name = "log10", description = "Returns the logarithm of `expr` with base 10"),
    one(name = "log1p", description = "Returns log(1 + `expr`)"),
    one(name = "log2", description = "Returns the logarithm of `expr` with base 2"),
    one(name = "lower", usage = "lower(str)", description = "Returns `str` with all characters changed to lowercase"),
    three(name = "lpad", usage = "lpad(str, len, pad)", description =
      """|Returns `str`, left-padded with pad to a length of len. If `str` is longer than len,
         |the return value is shortened to len characters""".stripMargin),
    many(name = "ltrim", maxArgs = 2, usage = "ltrim([trimStr, ]str)", description =
      "Removes the leading space characters from `str`"),
    many(name = "map", usage = "map(key0, value0, key1, value1, ...)", description =
      "Creates a map with the given key/value pairs"),
    many(name = "map_concat", usage = "map_concat(map, ...)", description =
      "Returns the union of all the given maps"),
    two(name = "map_from_arrays", usage = "map_from_arrays(keys, values)", description =
      "Creates a map with a pair of the given key/value arrays. All elements in keys should not be null"),
    many(name = "map_from_entries", usage = "map_from_entries(arrayOfEntries)", description =
      "Returns a map created from the given array of entries"),
    one(name = "map_keys", usage = "map_keys(map)", description =
      "Returns an unordered array containing the keys of the map"),
    one(name = "map_values", usage = "map_values(map)", description =
      "Returns an unordered array containing the values of the map"),
    one(name = "max", description = "Returns the maximum value of `expr`"),
    one(name = "md5", description = "Returns an MD5 128-bit checksum as a hex string of `expr`"),
    one(name = "mean", description = "Returns the mean calculated from values of a group"),
    one(name = "min", description = "Returns the minimum value of `expr`"),
    one(name = "minute", usage = "minute(timestamp)", description =
      "Returns the minute component of the string/timestamp"),
    zero(name = "monotonically_increasing_id", description =
      """|monotonically_increasing_id() - Returns monotonically increasing 64-bit integers.
         |The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
         |The current implementation puts the partition ID in the upper 31 bits, and the lower 33 bits
         |represent the record number within each partition. The assumption is that the data frame has
         |less than 1 billion partitions, and each partition has less than 8 billion records. The function
         |is non-deterministic because its result depends on partition IDs""".stripMargin),
    one(name = "month", usage = "month(date)", description = "Returns the month component of the date/timestamp"),
    many(name = "months_between", maxArgs = 3, usage = "months_between(timestamp1, timestamp2[, roundOff]) ", description =
      """|If timestamp1 is later than timestamp2, then the result is positive. If timestamp1 and timestamp2 are
         |on the same day of month, or both are the last day of month, time of day will be ignored. Otherwise,
         |the difference is calculated based on 31 days per month, and rounded to 8 digits unless roundOff=false""".stripMargin),
    many(name = "named_struct", usage = "named_struct(name1, val1, name2, val2, ...)", description =
      "Creates a struct with the given field names and values"),
    two(name = "nanvl", description = "Returns `expr1` if it's not NaN, or `expr2` otherwise"),
    one(name = "negative", description = "Returns the negated value of `expr`"),
    two(name = "next_day", usage = "next_day(start_date, day_of_week)", description =
      "Returns the first date which is later than start_date and named as indicated"),
    zero(name = "now", description = "Returns the current timestamp at the start of query evaluation"),
    one(name = "ntile", usage = "ntile(n)", description =
      "Divides the rows for each window partition into n buckets ranging from 1 to at most `n`"),
    two(name = "nullif", description = "Returns null if `expr1` equals to `expr2`, or `expr1` otherwise"),
    many(name = "parse_url", maxArgs = 3, usage = "parse_url(url, partToExtract[, key])", description =
      "Extracts a part from a URL"),

    // TODO add missing functions

    zero(name = "rank", usage = "rank() over(window_spec))", description =
      "Calculates the rank of a value in a group of values"),
    one(name = "reverse", usage = "reverse(array)", description =
      "Returns a reversed string or an array with reverse order of elements"),
    zero(name = "row_number", usage = "row_number() over(window_spec))", description =
      "Assigns a unique number to each row to which it is applied"),
    three(name = "rpad", usage = "rpad(str, len, pad)", description =
      """|Returns str, right-padded with pad to a length of len. If str is longer than len,
         |the return value is shortened to len characters""".stripMargin),
    one(name = "rtrim", usage = "rtrim(str)", description =
      "Removes the trailing space characters from `str`"),
    two(name = "split", usage = "split(str, regex)", description =
      "Splits `str` around occurrences that match `regex`"),
    one(name = "stddev", isAggregate = true, description =
      "Returns the sample standard deviation calculated from values of a group"),
    many(name = "substr", minArgs = 2, maxArgs = 3, usage = "substr(str, pos[, len])", description =
      """|Returns the substring of `str` that starts at pos and is of length `len`, or the slice of byte array that
         |starts at `pos` and is of length `len`""".stripMargin),
    many(name = "substring", minArgs = 2, maxArgs = 3, usage = "substring(str, pos[, len])", description =
      """|Returns the substring of str that starts at `pos` and is of length `len`, or the slice of byte array that
         |starts at pos and is of length `len`""".stripMargin),
    three(name = "substring_index", usage = "substring_index(str, delim, count)", description =
      """|Returns the substring from str before count occurrences of the delimiter delim. If count is positive,
         |everything to the left of the final delimiter (counting from the left) is returned. If count is negative,
         |everything to the right of the final delimiter (counting from the right) is returned. The function
         |substring_index performs a case-sensitive match when searching for  delim""".stripMargin),
    one(name = "sum", description = "Returns the sum calculated from values of a group", isAggregate = true),
    many(name = "to_date", maxArgs = 2, usage = "to_date(date_str[, fmt])", description =
      """|Parses the date_str expression with the fmt expression to a date. Returns null with invalid input.
         |By default, it follows casting rules to a date if the fmt is omitted""".stripMargin),
    three(name = "translate", usage = "translate(input, from, to)", description =
      """|Translates the input string by replacing the characters present in the from string with the
         |corresponding characters in the to string.""".stripMargin),
    one(name = "trim", usage = "trim(str)", description = "Removes the leading and trailing space characters from `str`"),
    one(name = "ucase", usage = "ucase(str)", description = "Returns `str` with all characters changed to uppercase"),
    one(name = "unbase64", usage = "unbase64(str)", description = "Converts the argument from a base 64 string `str` to a binary"),
    one(name = "unhex", description = "Converts hexadecimal expr to binary"),
    one(name = "upper", usage = "upper(str)", description = "Returns str with all characters changed to uppercase"),
    zero(name = "uuid", description =
      "Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string"),
    one(name = "var_pop", description = "Returns the population variance calculated from values of a group"),
    one(name = "var_samp", description = "Returns the sample variance calculated from values of a group"),
    one(name = "variance", description = "", isAggregate = true),
    one(name = "weekday", usage = "weekday(date)", description =
      "weekday(date) - Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)"),
    one(name = "weekofyear", usage = "weekofyear(date)", description =
      """|Returns the week of the year of the given date.
         |A week is considered to start on a Monday and week 1 is the first week with >3 days""".stripMargin),
    two(name = "xpath", usage = "xpath(xml, xpath)", description =
      "Returns a string array of values within the nodes of xml that match the XPath expression"),
    two(name = "xpath_boolean", usage = "xpath_boolean(xml, xpath)", description =
      "Returns true if the XPath expression evaluates to true, or if a matching node is found"),
    two(name = "xpath_double", usage = "xpath_double(xml, xpath)", description =
      "Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric"),
    two(name = "xpath_float", usage = "xpath_float(xml, xpath)", description =
      "Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric"),
    two(name = "xpath_int", usage = "xpath_int(xml, xpath)", description =
      " Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric"),
    two(name = "xpath_long", usage = "xpath_long(xml, xpath)", description =
      "Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric"),
    two(name = "xpath_number", usage = "xpath_number(xml, xpath)", description =
      "Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric"),
    two(name = "xpath_short", usage = "xpath_short(xml, xpath)", description =
      "Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric"),
    two(name = "xpath_string", usage = "xpath_string(xml, xpath)", description =
      "Returns the text contents of the first xml node that matches the XPath expression"),
    one(name = "year", usage = "year(date)", description = "Returns the year component of the date/timestamp"),
    three(name = "zip_with", usage = "zip_with(left, right, func)", description =
      """|Merges the two given arrays, element-wise, into a single array using function. If one array is shorter,
         |nulls are appended at the end to match the length of the longer array, before applying function""".stripMargin)
  ).map(f => f.name -> f): _*)

}