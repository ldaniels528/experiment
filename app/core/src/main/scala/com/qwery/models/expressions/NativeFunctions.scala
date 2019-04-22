package com.qwery.models.expressions

/**
  * Native SQL Functions Model Facades
  * @author lawrence.daniels@gmail.com
  * @note This is a generated class.
  */
trait NativeFunctions {

  /**
    * Returns the absolute value of the numeric value.
    * @example {{{ abs(expr) }}}
    */
  def abs(expr: Expression): Expression = FunctionCall("abs")(expr)

  /**
    * Returns the inverse cosine (a.k.a. arc cosine) of expr, as if computed by [[java.lang.Math#acos]].
    * @example {{{ acos(expr) }}}
    */
  def acos(expr: Expression): Expression = FunctionCall("acos")(expr)

  /**
    * Returns the date that is num_months after start_date.
    * @example {{{ add_months(start_date, num_months) }}}
    */
  def add_months(expr1: Expression, expr2: Expression): Expression = FunctionCall("add_months")(expr1, expr2)

  /**
    * Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state.
    * The final state is converted into the final result by applying a finish function.
    * @example {{{ aggregate(expr1, ...) }}}
    */
  def aggregate(expr: Expression*): Expression = FunctionCall("aggregate")(expr: _*)

  /**
    * Returns the estimated cardinality by HyperLogLog++. relativeSD defines the maximum estimation error allowed.
    * @example {{{ approx_count_distinct(expr1, ...) }}}
    */
  def approx_count_distinct(expr: Expression*): Expression = FunctionCall("approx_count_distinct")(expr: _*)

  /**
    * Returns the approximate percentile value of numeric column col at the given percentage.
    * The value of percentage must be between 0.0 and 1.0. The  accuracy parameter (default: 10000)
    * is a positive numeric literal which controls approximation accuracy at the cost of memory.
    * Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the approximation.
    * When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
    * In this case, returns the approximate percentile array of column col at the given percentage array.
    * @example {{{ approx_percentile(expr1, ...) }}}
    */
  def approx_percentile(expr: Expression*): Expression = FunctionCall("approx_percentile")(expr: _*)

  /**
    * Returns an array with the given elements.
    * @example {{{ array(expr1, ...) }}}
    */
  def array(expr: Expression*): Expression = FunctionCall("array")(expr: _*)

  /**
    * Returns true if the array contains the value.
    * @example {{{ array_contains(expr1, expr2) }}}
    */
  def array_contains(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_contains")(expr1, expr2)

  /**
    * Removes duplicate values from the array.
    * @example {{{ array_distinct(expr1, ...) }}}
    */
  def array_distinct(expr: Expression*): Expression = FunctionCall("array_distinct")(expr: _*)

  /**
    * Returns an array of the elements in array1 but not in array2, without duplicates.
    * @example {{{ array_except(expr1, expr2) }}}
    */
  def array_except(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_except")(expr1, expr2)

  /**
    * Returns an array of the elements in the intersection of array1 and array2, without duplicates.
    * @example {{{ array_index(expr1, expr2) }}}
    */
  def array_index(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_index")(expr1, expr2)

  /**
    * Returns an array of the elements in the intersection of array1 and array2, without duplicates.
    * @example {{{ array_intersect(array1, array2) }}}
    */
  def array_intersect(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_intersect")(expr1, expr2)

  /**
    * Concatenates the elements of the given array using the delimiter and an optional string to
    * replace nulls. If no value is set for nullReplacement, any null value is filtered.
    * @example {{{ array_join(array, delimiter[, nullReplacement]) }}}
    */
  def array_join(expr: Expression*): Expression = FunctionCall("array_join")(expr: _*)

  /**
    * Returns the maximum value in the array. NULL elements are skipped.
    * @example {{{ array_max(array) }}}
    */
  def array_max(expr: Expression): Expression = FunctionCall("array_max")(expr)

  /**
    * Returns the minimum value in the array. NULL elements are skipped.
    * @example {{{ array_min(array) }}}
    */
  def array_min(expr: Expression): Expression = FunctionCall("array_min")(expr)

  /**
    * Returns true if a1 contains at least a non-null element present also in a2. If the arrays have no
    * common element and they are both non-empty and either of them contains a null element null is returned,
    * false otherwise.
    * @example {{{ arrays_overlap(array1, array2) }}}
    */
  def array_overlap(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_overlap")(expr1, expr2)

  /**
    * Returns the (1-based) index of the first element of the array as long.
    * @example {{{ array_position(array, element) }}}
    */
  def array_position(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_position")(expr1, expr2)

  /**
    * Remove all elements that equal to element from array.
    * @example {{{ array_remove(array, element) }}}
    */
  def array_remove(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_remove")(expr1, expr2)

  /**
    * Returns the array containing element count times.
    * @example {{{ array_repeat(element, count) }}}
    */
  def array_repeat(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_repeat")(expr1, expr2)

  /**
    * Sorts the input array in ascending order. The elements of the input array must be orderable.
    * Null elements will be placed at the end of the returned array.
    * @example {{{ array_sort(array) }}}
    */
  def array_sort(expr: Expression): Expression = FunctionCall("array_sort")(expr)

  /**
    * Returns an array of the elements in the union of array1 and array2, without duplicates.
    * @example {{{ array_union(array1, array2) }}}
    */
  def array_union(expr1: Expression, expr2: Expression): Expression = FunctionCall("array_union")(expr1, expr2)

  /**
    * Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.
    * @example {{{ arrays_zip(array1, array2, ...) }}}
    */
  def arrays_zip(expr: Expression*): Expression = FunctionCall("arrays_zip")(expr: _*)

  /**
    * Returns the numeric value of the first character of string.
    * @example {{{ ascii(expr) }}}
    */
  def ascii(expr: Expression): Expression = FunctionCall("ascii")(expr)

  /**
    * Returns the inverse sine (a.k.a. arc sine) the arc sin of expr, as if computed by [[java.lang.Math#asin]].
    * @example {{{ asin(expr) }}}
    */
  def asin(expr: Expression): Expression = FunctionCall("asin")(expr)

  /**
    * Throws an exception if expr is not true.
    * @example {{{ assert_true(expr) }}}
    */
  def assert_true(expr: Expression): Expression = FunctionCall("assert_true")(expr)

  /**
    * Returns the inverse tangent (a.k.a. arc tangent) of expr, as if computed by [[java.lang.Math#atan]].
    * @example {{{ atan(expr) }}}
    */
  def atan(expr: Expression): Expression = FunctionCall("atan")(expr)

  /**
    * Returns the angle in radians between the positive x-axis of a plane and the point given by the
    * coordinates (exprX, exprY), as if computed by [[java.lang.Math#atan2]].
    * @example {{{ atan2(expr1, expr2) }}}
    */
  def atan2(expr1: Expression, expr2: Expression): Expression = FunctionCall("atan2")(expr1, expr2)

  /**
    * Returns the mean calculated from values of a group.
    * @example {{{ avg(expr) }}}
    */
  def avg(expr: Expression): Expression = FunctionCall("avg")(expr)

  /**
    * Converts the argument from a binary bin to a base 64 string.
    * @example {{{ base64(expr) }}}
    */
  def base64(expr: Expression): Expression = FunctionCall("base64")(expr)

  /**
    * Casts the value expr to the target data type bigint.
    * @example {{{ bigint(expr) }}}
    */
  def bigint(expr: Expression): Expression = FunctionCall("bigint")(expr)

  /**
    * Returns the string representation of the long value expr represented in binary.
    * @example {{{ bin(expr) }}}
    */
  def bin(expr: Expression): Expression = FunctionCall("bin")(expr)

  /**
    * Casts the value expr to the target data type binary.
    * @example {{{ binary(expr) }}}
    */
  def binary(expr: Expression): Expression = FunctionCall("binary")(expr)

  /**
    * Returns the bit length of string data or number of bits of binary data.
    * @example {{{ bit_length(expr) }}}
    */
  def bit_length(expr: Expression): Expression = FunctionCall("bit_length")(expr)

  /**
    * Casts the value expr to the target data type boolean.
    * @example {{{ boolean(expr) }}}
    */
  def boolean(expr: Expression): Expression = FunctionCall("boolean")(expr)

  /**
    * Returns expr rounded to d decimal places using HALF_EVEN rounding mode.
    * @example {{{ bround(expr, d) }}}
    */
  def bround(expr1: Expression, expr2: Expression): Expression = FunctionCall("bround")(expr1, expr2)

  /**
    * Returns the size of an array or a map. The function returns -1 if its input is null and
    *spark.sql.legacy.sizeOfNull is set to true. If spark.sql.legacy.sizeOfNull is set to false,
    * the function returns null for null input. By default, the spark.sql.legacy.sizeOfNull parameter
    * is set to true.
    * @example {{{ cardinality(expr) }}}
    */
  def cardinality(expr: Expression): Expression = FunctionCall("cardinality")(expr)

  /**
    * Returns the cube root of expr.
    * @example {{{ cbrt(expr) }}}
    */
  def cbrt(expr: Expression): Expression = FunctionCall("cbrt")(expr)

  /**
    * Returns the smallest integer not smaller than expr.
    * @example {{{ ceil(expr) }}}
    */
  def ceil(expr: Expression): Expression = FunctionCall("ceil")(expr)

  /**
    * Returns the smallest integer not smaller than expr.
    * @example {{{ ceiling(expr) }}}
    */
  def ceiling(expr: Expression): Expression = FunctionCall("ceiling")(expr)

  /**
    * Returns the ASCII character having the binary equivalent to expr.
    * If n is larger than 256 the result is equivalent to chr(n % 256).
    * @example {{{ char(expr) }}}
    */
  def char(expr: Expression): Expression = FunctionCall("char")(expr)

  /**
    * Returns the character length of string data or number of bytes of binary data. The length of
    * string data includes the trailing spaces. The length of binary data includes binary zeros.
    * @example {{{ char_length(expr) }}}
    */
  def char_length(expr: Expression): Expression = FunctionCall("char_length")(expr)

  /**
    * Collects and returns a list of non-unique elements.
    * @example {{{ character_length(expr) }}}
    */
  def character_length(expr: Expression): Expression = FunctionCall("character_length")(expr)

  /**
    * Returns the ASCII character having the binary equivalent to expr.
    * If n is larger than 256 the result is equivalent to chr(n % 256).
    * @example {{{ chr(expr) }}}
    */
  def chr(expr: Expression): Expression = FunctionCall("chr")(expr)

  /**
    * Returns the first non-null argument if exists. Otherwise, null.
    * @example {{{ coalesce(expr1, ...) }}}
    */
  def coalesce(expr: Expression*): Expression = FunctionCall("coalesce")(expr: _*)

  /**
    * Collects and returns a list of non-unique elements.
    * @example {{{ collect_list(expr) }}}
    */
  def collect_list(expr: Expression): Expression = FunctionCall("collect_list")(expr)

  /**
    * Collects and returns a set of unique elements.
    * @example {{{ collect_set(expr) }}}
    */
  def collect_set(expr: Expression): Expression = FunctionCall("collect_set")(expr)

  /**
    * Returns the concatenation of col1, col2, ..., colN.
    * @example {{{ concat(col1, col2, ..., colN) }}}
    */
  def concat(expr: Expression*): Expression = FunctionCall("concat")(expr: _*)

  /**
    * Returns the concatenation of the strings separated by sep.
    * @example {{{ concat_ws(sep, [str | array(str)]+) }}}
    */
  def concat_ws(expr: Expression*): Expression = FunctionCall("concat_ws")(expr: _*)

  /**
    * Convert num from from_base to to_base.
    * @example {{{ conv(num, from_base, to_base) }}}
    */
  def conv(expr: Expression*): Expression = FunctionCall("conv")(expr: _*)

  /**
    * Returns Pearson coefficient of correlation between a set of number pairs.
    * @example {{{ corr(expr1, expr2) }}}
    */
  def corr(expr1: Expression, expr2: Expression): Expression = FunctionCall("corr")(expr1, expr2)

  /**
    * Returns the cosine of expr, as if computed by [[java.lang.Math#cos]].
    * @example {{{ cos(expr) }}}
    */
  def cos(expr: Expression): Expression = FunctionCall("cos")(expr)

  /**
    * Returns the hyperbolic cosine of expr, as if computed by [[java.lang.Math#cosh]].
    * @example {{{ cosh(expr) }}}
    */
  def cosh(expr: Expression): Expression = FunctionCall("cosh")(expr)

  /**
    * Returns the cotangent of expr, as if computed by [[java.lang.Math#cot]].
    * @example {{{ cot(expr) }}}
    */
  def cot(expr: Expression): Expression = FunctionCall("cot")(expr)

  /**
    * Returns the total number of retrieved rows, including rows containing null.
    * @example {{{ count(expr1, ...) }}}
    */
  def count(expr: Expression*): Expression = FunctionCall("count")(expr: _*)

  /**
    * Returns a count-min sketch of a column with the given esp, confidence and seed.
    * The result is an array of bytes, which can be deserialized to a CountMinSketch before usage.
    * Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.
    * @example {{{ count_min_sketch(col, eps, confidence, seed) }}}
    */
  def count_min_sketch(expr: Expression*): Expression = FunctionCall("count_min_sketch")(expr: _*)

  /**
    * Returns the population covariance of a set of number pairs.
    * @example {{{ covar_pop(expr1, expr2) }}}
    */
  def covar_pop(expr1: Expression, expr2: Expression): Expression = FunctionCall("covar_pop")(expr1, expr2)

  /**
    * Returns the sample covariance of a set of number pairs.
    * @example {{{ covar_samp(expr1, expr2) }}}
    */
  def covar_samp(expr1: Expression, expr2: Expression): Expression = FunctionCall("covar_samp")(expr1, expr2)

  /**
    * Returns a cyclic redundancy check value of the expr as a bigint.
    * @example {{{ crc32(expr) }}}
    */
  def crc32(expr: Expression): Expression = FunctionCall("crc32")(expr)

  /**
    * [[https://spark.apache.org/docs/2.4.0/api/sql/index.html#cube]].
    * @example {{{ cube(expr1, ...) }}}
    */
  def cube(expr: Expression*): Expression = FunctionCall("cube")(expr: _*)

  /**
    * Computes the position of a value relative to all values in the partition.
    * @example {{{ cume_dist() }}}
    */
  def cume_dist(): Expression = FunctionCall("cume_dist")()

  /**
    * Returns the current database.
    * @example {{{ current_database() }}}
    */
  def current_database(): Expression = FunctionCall("current_database")()

  /**
    * Returns the current date at the start of query evaluation.
    * @example {{{ current_date() }}}
    */
  def current_date(): Expression = FunctionCall("current_date")()

  /**
    * Returns the current timestamp at the start of query evaluation.
    * @example {{{ current_timestamp() }}}
    */
  def current_timestamp(): Expression = FunctionCall("current_timestamp")()

  /**
    * Casts the value expr to the target data type date.
    * @example {{{ date(expr) }}}
    */
  def date(expr: Expression): Expression = FunctionCall("date")(expr)

  /**
    * Returns the date that is num_days after start_date.
    * @example {{{ date_add(start_date, num_days) }}}
    */
  def date_add(expr1: Expression, expr2: Expression): Expression = FunctionCall("date_add")(expr1, expr2)

  /**
    * Converts timestamp to a value of string in the format specified by the date format fmt.
    * @example {{{ date_format(timestamp, fmt) }}}
    */
  def date_format(expr1: Expression, expr2: Expression): Expression = FunctionCall("date_format")(expr1, expr2)

  /**
    * Returns the date that is num_days before start_date.
    * @example {{{ date_sub(start_date, num_days) }}}
    */
  def date_sub(expr1: Expression, expr2: Expression): Expression = FunctionCall("date_sub")(expr1, expr2)

  /**
    * Returns timestamp ts truncated to the unit specified by the format model fmt.
    * fmt should be one of ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD",
    * "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"].
    * @example {{{ date_trunc(expr1, expr2) }}}
    */
  def date_trunc(expr1: Expression, expr2: Expression): Expression = FunctionCall("date_trunc")(expr1, expr2)

  /**
    * Returns the number of days from startDate to endDate.
    * @example {{{ datediff(endDate, startDate) }}}
    */
  def datediff(expr1: Expression, expr2: Expression): Expression = FunctionCall("datediff")(expr1, expr2)

  /**
    * Returns the day of month of the date/timestamp.
    * @example {{{ day(date) }}}
    */
  def day(expr: Expression): Expression = FunctionCall("day")(expr)

  /**
    * Returns the day of month of the date/timestamp.
    * @example {{{ dayofmonth(date) }}}
    */
  def dayofmonth(expr: Expression): Expression = FunctionCall("dayofmonth")(expr)

  /**
    * Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
    * @example {{{ dayofweek(date) }}}
    */
  def dayofweek(expr: Expression): Expression = FunctionCall("dayofweek")(expr)

  /**
    * Casts the value expr to the target data type decimal.
    * @example {{{ decimal(expr) }}}
    */
  def decimal(expr: Expression): Expression = FunctionCall("decimal")(expr)

  /**
    * Decodes the first argument using the second argument character set.
    * @example {{{ decode(bin, charset) }}}
    */
  def decode(expr1: Expression, expr2: Expression): Expression = FunctionCall("decode")(expr1, expr2)

  /**
    * Converts radians to degrees.
    * @example {{{ degrees(expr) }}}
    */
  def degrees(expr: Expression): Expression = FunctionCall("degrees")(expr)

  /**
    * Computes the rank of a value in a group of values. The result is one plus the previously assigned rank value.
    * Unlike the function rank, dense_rank will not produce gaps in the ranking sequence.
    * @example {{{ dense_rank() }}}
    */
  def dense_rank(): Expression = FunctionCall("dense_rank")()

  /**
    * .
    * @example {{{ distinct(expr1, ...) }}}
    */
  def distinct(expr: Expression*): Expression = FunctionCall("distinct")(expr: _*)

  /**
    * Casts the value expr to the target data type double.
    * @example {{{ double(expr) }}}
    */
  def double(expr: Expression): Expression = FunctionCall("double")(expr)

  /**
    * Returns Euler's number, e.
    * @example {{{ e() }}}
    */
  def e(): Expression = FunctionCall("e")()

  /**
    * Returns the n-th input, e.g., returns input2 when n is 2.
    * @example {{{ elt(n, input1, input2, ...) }}}
    */
  def elt(expr: Expression*): Expression = FunctionCall("elt")(expr: _*)

  /**
    * Encodes the first argument using the second argument character set.
    * @example {{{ encode(str, charset) }}}
    */
  def encode(expr1: Expression, expr2: Expression): Expression = FunctionCall("encode")(expr1, expr2)

  /**
    * Returns e to the power of expr.
    * @example {{{ exp(expr) }}}
    */
  def exp(expr: Expression): Expression = FunctionCall("exp")(expr)

  /**
    * Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.
    * @example {{{ explode(expr) }}}
    */
  def explode(expr: Expression): Expression = FunctionCall("explode")(expr)

  /**
    * Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.
    * @example {{{ explode_outer(expr) }}}
    */
  def explode_outer(expr: Expression): Expression = FunctionCall("explode_outer")(expr)

  /**
    * Returns exp(expr) - 1.
    * @example {{{ expm1(expr) }}}
    */
  def expm1(expr: Expression): Expression = FunctionCall("expm1")(expr)

  /**
    * Returns the factorial of expr. expr is [0..20]. Otherwise, null.
    * @example {{{ factorial(expr) }}}
    */
  def factorial(expr: Expression): Expression = FunctionCall("factorial")(expr)

  /**
    * Returns the index (1-based) of the given string (str) in the comma-delimited list (str_array).
    * Returns 0, if the string was not found or if the given string (str) contains a comma.
    * @example {{{ find_in_set(expr1, expr2) }}}
    */
  def find_in_set(expr: Expression*): Expression = FunctionCall("find_in_set")(expr: _*)

  /**
    * Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values.
    * @example {{{ first(expr[, isIgnoreNull]) }}}
    */
  def first(expr: Expression*): Expression = FunctionCall("first")(expr: _*)

  /**
    * Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values.
    * @example {{{ first_value(expr[, isIgnoreNull]) }}}
    */
  def first_value(expr: Expression*): Expression = FunctionCall("first_value")(expr: _*)

  /**
    * Transforms an array of arrays into a single array.
    * @example {{{ flatten(arrayOfArrays) }}}
    */
  def flatten(expr: Expression): Expression = FunctionCall("flatten")(expr)

  /**
    * Casts the value expr to the target data type float.
    * @example {{{ float(expr) }}}
    */
  def float(expr: Expression): Expression = FunctionCall("float")(expr)

  /**
    * Returns the largest integer not greater than expr.
    * @example {{{ floor(expr) }}}
    */
  def floor(expr: Expression): Expression = FunctionCall("floor")(expr)

  /**
    * Formats the number expr1 like '#,###,###.##', rounded to expr2 decimal places.
    * If expr2 is 0, the result has no decimal point or fractional part. expr2 also accept a user specified format.
    * This is supposed to function like MySQL's FORMAT.
    * @example {{{ format_number(expr1, expr2) }}}
    */
  def format_number(expr1: Expression, expr2: Expression): Expression = FunctionCall("format_number")(expr1, expr2)

  /**
    * Returns a formatted string from printf-style format strings.
    * @example {{{ format_string(strfmt, obj, ...) }}}
    */
  def format_string(expr: Expression*): Expression = FunctionCall("format_string")(expr: _*)

  /**
    * Returns a struct value with the given jsonStr and schema.
    * @example {{{ from_json(jsonStr, schema[, options]) }}}
    */
  def from_json(expr: Expression*): Expression = FunctionCall("from_json")(expr: _*)

  /**
    * Returns unix_time in the specified format.
    * @example {{{ from_unixtime(unix_time, format) }}}
    */
  def from_unixtime(expr1: Expression, expr2: Expression): Expression = FunctionCall("from_unixtime")(expr1, expr2)

  /**
    * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC,
    * and renders that time as a timestamp in the given time zone.
    * For example, 'GMT+1' would yield '2017-07-14 03:40:00.0'.
    * @example {{{ from_utc_timestamp(timestamp, timezone) }}}
    */
  def from_utc_timestamp(expr1: Expression, expr2: Expression): Expression = FunctionCall("from_utc_timestamp")(expr1, expr2)

  /**
    * Extracts a json object from path.
    * @example {{{ get_json_object(json_txt, path) }}}
    */
  def get_json_object(expr1: Expression, expr2: Expression): Expression = FunctionCall("get_json_object")(expr1, expr2)

  /**
    * Returns the greatest value of all parameters, skipping null values.
    * @example {{{ greatest(expr1, ...) }}}
    */
  def greatest(expr: Expression*): Expression = FunctionCall("greatest")(expr: _*)

  /**
    * Returns a hash value of the arguments.
    * @example {{{ hash(expr1, ...) }}}
    */
  def hash(expr: Expression*): Expression = FunctionCall("hash")(expr: _*)

  /**
    * Converts expr to hexadecimal.
    * @example {{{ hex(expr) }}}
    */
  def hex(expr: Expression): Expression = FunctionCall("hex")(expr)

  /**
    * Returns the hour component of the string/timestamp.
    * @example {{{ hour(timestamp) }}}
    */
  def hour(expr: Expression): Expression = FunctionCall("hour")(expr)

  /**
    * Returns sqrt(expr1^2 + expr2^2).
    * @example {{{ hypot(expr1, expr2) }}}
    */
  def hypot(expr1: Expression, expr2: Expression): Expression = FunctionCall("hypot")(expr1, expr2)

  /**
    * If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.
    * @example {{{ if(expr1, expr2, expr3) }}}
    */
  def `if`(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("if")(expr1, expr2, expr3)

  /**
    * Returns expr2 if expr1 is null, or expr1 otherwise.
    * @example {{{ ifnull(expr1, expr2) }}}
    */
  def ifnull(expr1: Expression, expr2: Expression): Expression = FunctionCall("ifnull")(expr1, expr2)

  /**
    * Returns true if expr equals to any valN.
    * @example {{{ in(expr1, ...) }}}
    */
  def in(expr: Expression*): Expression = FunctionCall("in")(expr: _*)

  /**
    * Returns str with the first letter of each word in uppercase. All other letters are in lowercase.
    * Words are delimited by white space.
    * @example {{{ initcap(str) }}}
    */
  def initcap(expr: Expression): Expression = FunctionCall("initcap")(expr)

  /**
    * Explodes an array of structs into a table.
    * @example {{{ inline(expr) }}}
    */
  def inline(expr: Expression): Expression = FunctionCall("inline")(expr)

  /**
    * Explodes an array of structs into a table.
    * @example {{{ inline_outer(expr) }}}
    */
  def inline_outer(expr: Expression): Expression = FunctionCall("inline_outer")(expr)

  /**
    * Returns the length of the block being read, or -1 if not available.
    * @example {{{ input_file_block_length() }}}
    */
  def input_file_block_length(): Expression = FunctionCall("input_file_block_length")()

  /**
    * Returns the start offset of the block being read, or -1 if not available.
    * @example {{{ input_file_block_start() }}}
    */
  def input_file_block_start(): Expression = FunctionCall("input_file_block_start")()

  /**
    * Returns the name of the file being read, or empty string if not available.
    * @example {{{ input_file_name() }}}
    */
  def input_file_name(): Expression = FunctionCall("input_file_name")()

  /**
    * Returns the (1-based) index of the first occurrence of substr in str.
    * @example {{{ instr(str, substr) }}}
    */
  def instr(expr1: Expression, expr2: Expression): Expression = FunctionCall("instr")(expr1, expr2)

  /**
    * Casts the value expr to the target data type int.
    * @example {{{ int(expr) }}}
    */
  def int(expr: Expression): Expression = FunctionCall("int")(expr)

  /**
    * Returns true if expr is NaN, or false otherwise.
    * @example {{{ isnan(expr) }}}
    */
  def isnan(expr: Expression): Expression = FunctionCall("isnan")(expr)

  /**
    * Returns true if expr is not null, or false otherwise.
    * @example {{{ isnotnull(expr) }}}
    */
  def isnotnull(expr: Expression): Expression = FunctionCall("isnotnull")(expr)

  /**
    * Returns true if expr is null, or false otherwise.
    * @example {{{ isnull(expr) }}}
    */
  def isnull(expr: Expression): Expression = FunctionCall("isnull")(expr)

  /**
    * Calls a method with reflection.
    * @example {{{ java_method(class, method[, arg1[, arg2 ..]]) }}}
    */
  def java_method(expr: Expression*): Expression = FunctionCall("java_method")(expr: _*)

  /**
    * Returns a tuple like the function get_json_object, but it takes multiple names.
    * All the input parameters and output column types are string.
    * @example {{{ json_tuple(jsonStr, p1, p2, ..., pn) }}}
    */
  def json_tuple(expr: Expression*): Expression = FunctionCall("json_tuple")(expr: _*)

  /**
    * Returns the kurtosis value calculated from values of a group.
    * @example {{{ kurtosis(expr) }}}
    */
  def kurtosis(expr: Expression): Expression = FunctionCall("kurtosis")(expr)

  /**
    * Returns the value of input at the offsetth row before the current row in the window. The default value of
    * offset is 1 and the default value of default is null. If the value of  input at the offsetth row is null,
    * null is returned. If there is no such offset row (e.g., when the offset is 1, the first row of the window
    * does not have any previous row), default is returned.
    * @example {{{ lag(input[, offset[, default]]) }}}
    */
  def lag(expr: Expression*): Expression = FunctionCall("lag")(expr: _*)

  /**
    * Returns the last value of expr for a group of rows. If isIgnoreNull is true,
    * returns only non-null values.
    * @example {{{ last(expr[, isIgnoreNull]) }}}
    */
  def last(expr: Expression*): Expression = FunctionCall("last")(expr: _*)

  /**
    * Returns the last day of the month which the date belongs to.
    * @example {{{ last_day(date) }}}
    */
  def last_day(expr: Expression): Expression = FunctionCall("last_day")(expr)

  /**
    * Returns the last value of expr for a group of rows. If isIgnoreNull is true,
    * returns only non-null values.
    * @example {{{ last_value(expr[, isIgnoreNull]) }}}
    */
  def last_value(expr: Expression*): Expression = FunctionCall("last_value")(expr: _*)

  /**
    * Returns str with all characters changed to lowercase.
    * @example {{{ lcase(str) }}}
    */
  def lcase(expr: Expression): Expression = FunctionCall("lcase")(expr)

  /**
    * Returns the value of input at the offsetth row after the current row in the window. The default value of
    * offset is 1 and the default value of default is null. If the value of  input at the offsetth row is null,
    * null is returned. If there is no such an offset row (e.g., when the offset is 1, the last row of the window
    * does not have any subsequent row), default is returned.
    * @example {{{ lead(input[, offset[, default]])  }}}
    */
  def lead(expr: Expression*): Expression = FunctionCall("lead")(expr: _*)

  /**
    * Returns the least value of all parameters, skipping null values.
    * @example {{{ least(expr1, ...) }}}
    */
  def least(expr: Expression*): Expression = FunctionCall("least")(expr: _*)

  /**
    * Returns the leftmost len(len can be string type) characters from the string str,
    * if len is less or equal than 0 the result is an empty string.
    * @example {{{ left(str, len) }}}
    */
  def left(expr1: Expression, expr2: Expression): Expression = FunctionCall("left")(expr1, expr2)

  /**
    * Returns the character length of string data or number of bytes of binary data. The length of string data
    * includes the trailing spaces. The length of binary data includes binary zeros.
    * @example {{{ length(expr) }}}
    */
  def length(expr: Expression): Expression = FunctionCall("length")(expr)

  /**
    * Returns the Levenshtein distance between the two given strings.
    * @example {{{ levenshtein(str1, str2) }}}
    */
  def levenshtein(expr1: Expression, expr2: Expression): Expression = FunctionCall("levenshtein")(expr1, expr2)

  /**
    * Returns the natural logarithm (base e) of `expr`.
    * @example {{{ ln(expr) }}}
    */
  def ln(expr: Expression): Expression = FunctionCall("ln")(expr)

  /**
    * Returns the position of the first occurrence of substr in str after position pos.
    * The given pos and return value are 1-based.
    * @example {{{ locate(substr, str[, pos]) }}}
    */
  def locate(expr: Expression*): Expression = FunctionCall("locate")(expr: _*)

  /**
    * Returns the logarithm of `expr` with `base`.
    * @example {{{ log(base, expr) }}}
    */
  def log(expr1: Expression, expr2: Expression): Expression = FunctionCall("log")(expr1, expr2)

  /**
    * Returns the logarithm of `expr` with base 10.
    * @example {{{ log10(expr) }}}
    */
  def log10(expr: Expression): Expression = FunctionCall("log10")(expr)

  /**
    * Returns log(1 + `expr`).
    * @example {{{ log1p(expr) }}}
    */
  def log1p(expr: Expression): Expression = FunctionCall("log1p")(expr)

  /**
    * Returns the logarithm of `expr` with base 2.
    * @example {{{ log2(expr) }}}
    */
  def log2(expr: Expression): Expression = FunctionCall("log2")(expr)

  /**
    * Returns `str` with all characters changed to lowercase.
    * @example {{{ lower(str) }}}
    */
  def lower(expr: Expression): Expression = FunctionCall("lower")(expr)

  /**
    * Returns `str`, left-padded with pad to a length of len. If `str` is longer than len,
    * the return value is shortened to len characters.
    * @example {{{ lpad(str, len, pad) }}}
    */
  def lpad(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("lpad")(expr1, expr2, expr3)

  /**
    * Removes the leading space characters from `str`.
    * @example {{{ ltrim([trimStr, ]str) }}}
    */
  def ltrim(expr: Expression*): Expression = FunctionCall("ltrim")(expr: _*)

  /**
    * Creates a map with the given key/value pairs.
    * @example {{{ map(key0, value0, key1, value1, ...) }}}
    */
  def map(expr: Expression*): Expression = FunctionCall("map")(expr: _*)

  /**
    * Returns the union of all the given maps.
    * @example {{{ map_concat(map, ...) }}}
    */
  def map_concat(expr: Expression*): Expression = FunctionCall("map_concat")(expr: _*)

  /**
    * Creates a map with a pair of the given key/value arrays. All elements in keys should not be null.
    * @example {{{ map_from_arrays(keys, values) }}}
    */
  def map_from_arrays(expr1: Expression, expr2: Expression): Expression = FunctionCall("map_from_arrays")(expr1, expr2)

  /**
    * Returns a map created from the given array of entries.
    * @example {{{ map_from_entries(arrayOfEntries) }}}
    */
  def map_from_entries(expr: Expression*): Expression = FunctionCall("map_from_entries")(expr: _*)

  /**
    * Returns an unordered array containing the keys of the map.
    * @example {{{ map_keys(map) }}}
    */
  def map_keys(expr: Expression): Expression = FunctionCall("map_keys")(expr)

  /**
    * Returns an unordered array containing the values of the map.
    * @example {{{ map_values(map) }}}
    */
  def map_values(expr: Expression): Expression = FunctionCall("map_values")(expr)

  /**
    * Returns the maximum value of `expr`.
    * @example {{{ max(expr) }}}
    */
  def max(expr: Expression): Expression = FunctionCall("max")(expr)

  /**
    * Returns an MD5 128-bit checksum as a hex string of `expr`.
    * @example {{{ md5(expr) }}}
    */
  def md5(expr: Expression): Expression = FunctionCall("md5")(expr)

  /**
    * Returns the mean calculated from values of a group.
    * @example {{{ mean(expr) }}}
    */
  def mean(expr: Expression): Expression = FunctionCall("mean")(expr)

  /**
    * Returns the minimum value of `expr`.
    * @example {{{ min(expr) }}}
    */
  def min(expr: Expression): Expression = FunctionCall("min")(expr)

  /**
    * Returns the minute component of the string/timestamp.
    * @example {{{ minute(timestamp) }}}
    */
  def minute(expr: Expression): Expression = FunctionCall("minute")(expr)

  /**
    * Returns the remainder after expr1/expr2.
    * @example {{{ expr1 mod expr2 }}}
    */
  def mod(expr1: Expression, expr2: Expression): Expression = FunctionCall("mod")(expr1, expr2)

  /**
    * monotonically_increasing_id() - Returns monotonically increasing 64-bit integers.
    * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    * The current implementation puts the partition ID in the upper 31 bits, and the lower 33 bits
    * represent the record number within each partition. The assumption is that the data frame has
    * less than 1 billion partitions, and each partition has less than 8 billion records. The function
    * is non-deterministic because its result depends on partition IDs.
    * @example {{{ monotonically_increasing_id() }}}
    */
  def monotonically_increasing_id(): Expression = FunctionCall("monotonically_increasing_id")()

  /**
    * Returns the month component of the date/timestamp.
    * @example {{{ month(date) }}}
    */
  def month(expr: Expression): Expression = FunctionCall("month")(expr)

  /**
    * If timestamp1 is later than timestamp2, then the result is positive. If timestamp1 and timestamp2 are
    * on the same day of month, or both are the last day of month, time of day will be ignored. Otherwise,
    * the difference is calculated based on 31 days per month, and rounded to 8 digits unless roundOff=false.
    * @example {{{ months_between(timestamp1, timestamp2[, roundOff])  }}}
    */
  def months_between(expr: Expression*): Expression = FunctionCall("months_between")(expr: _*)

  /**
    * Creates a struct with the given field names and values.
    * @example {{{ named_struct(name1, val1, name2, val2, ...) }}}
    */
  def named_struct(expr: Expression*): Expression = FunctionCall("named_struct")(expr: _*)

  /**
    * Returns `expr1` if it's not NaN, or `expr2` otherwise.
    * @example {{{ nanvl(expr1, expr2) }}}
    */
  def nanvl(expr1: Expression, expr2: Expression): Expression = FunctionCall("nanvl")(expr1, expr2)

  /**
    * Returns the negated value of `expr`.
    * @example {{{ negative(expr) }}}
    */
  def negative(expr: Expression): Expression = FunctionCall("negative")(expr)

  /**
    * Returns the first date which is later than start_date and named as indicated.
    * @example {{{ next_day(start_date, day_of_week) }}}
    */
  def next_day(expr1: Expression, expr2: Expression): Expression = FunctionCall("next_day")(expr1, expr2)

  /**
    * Returns the current timestamp at the start of query evaluation.
    * @example {{{ now() }}}
    */
  def now(): Expression = FunctionCall("now")()

  /**
    * Divides the rows for each window partition into n buckets ranging from 1 to at most `n`.
    * @example {{{ ntile(n) }}}
    */
  def ntile(expr: Expression): Expression = FunctionCall("ntile")(expr)

  /**
    * Returns null if `expr1` equals to `expr2`, or `expr1` otherwise.
    * @example {{{ nullif(expr1, expr2) }}}
    */
  def nullif(expr1: Expression, expr2: Expression): Expression = FunctionCall("nullif")(expr1, expr2)

  /**
    * Extracts a part from a URL.
    * @example {{{ parse_url(url, partToExtract[, key]) }}}
    */
  def parse_url(expr: Expression*): Expression = FunctionCall("parse_url")(expr: _*)

  /**
    * Raises expr1 to the power of expr2.
    * @example {{{ pow(expr1, expr2) }}}
    */
  def pow(expr1: Expression, expr2: Expression): Expression = FunctionCall("pow")(expr1, expr2)

  /**
    * Raises expr1 to the power of expr2.
    * @example {{{ power(expr1, expr2) }}}
    */
  def power(expr1: Expression, expr2: Expression): Expression = FunctionCall("power")(expr1, expr2)

  /**
    * Returns a formatted string from printf-style format strings.
    * @example {{{ printf(strfmt, obj, ...) }}}
    */
  def printf(expr: Expression*): Expression = FunctionCall("printf")(expr: _*)

  /**
    * Returns the quarter of the year for date, in the range 1 to 4.
    * @example {{{ quarter(date) }}}
    */
  def quarter(expr: Expression): Expression = FunctionCall("quarter")(expr)

  /**
    * Converts degrees to radians.
    * @example {{{ radians(expr) }}}
    */
  def radians(expr: Expression): Expression = FunctionCall("radians")(expr)

  /**
    * Returns a random value with independent and identically distributed (i.i.d.) uniformly distributed values in [0, 1).
    * @example {{{ rand([seed]) }}}
    */
  def rand(expr: Expression): Expression = FunctionCall("rand")(expr)

  /**
    * Returns a random value with independent and identically distributed (i.i.d.) values drawn from the standard normal distribution.
    * @example {{{ randn([seed]) }}}
    */
  def randn(expr: Expression): Expression = FunctionCall("randn")(expr)

  /**
    * Computes the rank of a value in a group of values. The result is one plus the number of rows preceding or
    * equal to the current row in the ordering of the partition. The values will produce gaps in the sequence.
    * @example {{{ rank() over(window_spec)) }}}
    */
  def rank(): Expression = FunctionCall("rank")()

  /**
    * Calls a method with reflection.
    * @example {{{ reflect(class, method[, arg1[, arg2 ..]]) }}}
    */
  def reflect(expr: Expression*): Expression = FunctionCall("reflect")(expr: _*)

  /**
    * Extracts a group that matches `regexp`.
    * @example {{{ regexp_extract(str, regexp[, idx]) }}}
    */
  def regexp_extract(expr: Expression*): Expression = FunctionCall("regexp_extract")(expr: _*)

  /**
    * Replaces all substrings of `str` that match `regexp` with `rep`.
    * @example {{{ regexp_replace(str, regexp, rep) }}}
    */
  def regexp_replace(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("regexp_replace")(expr1, expr2, expr3)

  /**
    * Returns the string which repeats the given string value n times.
    * @example {{{ repeat(str, n) }}}
    */
  def repeat(expr1: Expression, expr2: Expression): Expression = FunctionCall("repeat")(expr1, expr2)

  /**
    * Replaces all occurrences of `search` with `replace`.
    * @example {{{ replace(str, search[, replace]) }}}
    */
  def replace(expr: Expression*): Expression = FunctionCall("replace")(expr: _*)

  /**
    * Returns a reversed string or an array with reverse order of elements.
    * @example {{{ reverse(array) }}}
    */
  def reverse(expr: Expression): Expression = FunctionCall("reverse")(expr)

  /**
    * Returns the rightmost `len`(`len` can be string type) characters from the string `str`,
    * if `len` is less or equal than 0 the result is an empty string.
    * @example {{{ right(str, len) }}}
    */
  def right(expr1: Expression, expr2: Expression): Expression = FunctionCall("right")(expr1, expr2)

  /**
    * Returns the double value that is closest in value to the argument and is equal to a mathematical integer.
    * @example {{{ rint(expr) }}}
    */
  def rint(expr: Expression): Expression = FunctionCall("rint")(expr)

  /**
    * TODO documentation.
    * @example {{{ rollup() }}}
    */
  def rollup(): Expression = FunctionCall("rollup")()

  /**
    * Returns `expr` rounded to `d` decimal places using `HALF_UP` rounding mode.
    * @example {{{ round(expr, d) }}}
    */
  def round(expr1: Expression, expr2: Expression): Expression = FunctionCall("round")(expr1, expr2)

  /**
    * Assigns a unique number to each row to which it is applied.
    * @example {{{ row_number() over(window_spec)) }}}
    */
  def row_number(): Expression = FunctionCall("row_number")()

  /**
    * Returns str, right-padded with pad to a length of len. If str is longer than len,
    * the return value is shortened to len characters.
    * @example {{{ rpad(str, len, pad) }}}
    */
  def rpad(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("rpad")(expr1, expr2, expr3)

  /**
    * Removes the trailing space characters from `str`.
    * @example {{{ rtrim(str) }}}
    */
  def rtrim(expr: Expression): Expression = FunctionCall("rtrim")(expr)

  /**
    * Splits `str` around occurrences that match `regex`.
    * @example {{{ split(str, regex) }}}
    */
  def split(expr1: Expression, expr2: Expression): Expression = FunctionCall("split")(expr1, expr2)

  /**
    * Returns the sample standard deviation calculated from values of a group.
    * @example {{{ stddev(expr) }}}
    */
  def stddev(expr: Expression): Expression = FunctionCall("stddev")(expr)

  /**
    * Returns the substring of `str` that starts at pos and is of length `len`, or the slice of byte array that
    * starts at `pos` and is of length `len`.
    * @example {{{ substr(str, pos[, len]) }}}
    */
  def substr(expr: Expression*): Expression = FunctionCall("substr")(expr: _*)

  /**
    * Returns the substring of str that starts at `pos` and is of length `len`, or the slice of byte array that
    * starts at pos and is of length `len`.
    * @example {{{ substring(str, pos[, len]) }}}
    */
  def substring(expr: Expression*): Expression = FunctionCall("substring")(expr: _*)

  /**
    * Returns the substring from str before count occurrences of the delimiter delim. If count is positive,
    * everything to the left of the final delimiter (counting from the left) is returned. If count is negative,
    * everything to the right of the final delimiter (counting from the right) is returned. The function
    * substring_index performs a case-sensitive match when searching for  delim.
    * @example {{{ substring_index(str, delim, count) }}}
    */
  def substring_index(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("substring_index")(expr1, expr2, expr3)

  /**
    * Returns the sum calculated from values of a group.
    * @example {{{ sum(expr) }}}
    */
  def sum(expr: Expression): Expression = FunctionCall("sum")(expr)

  /**
    * Parses the date_str expression with the fmt expression to a date. Returns null with invalid input.
    * By default, it follows casting rules to a date if the fmt is omitted.
    * @example {{{ to_date(date_str[, fmt]) }}}
    */
  def to_date(expr: Expression*): Expression = FunctionCall("to_date")(expr: _*)

  /**
    * Translates the input string by replacing the characters present in the from string with the
    * corresponding characters in the to string..
    * @example {{{ translate(input, from, to) }}}
    */
  def translate(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("translate")(expr1, expr2, expr3)

  /**
    * Removes the leading and trailing space characters from `str`.
    * @example {{{ trim(str) }}}
    */
  def trim(expr: Expression): Expression = FunctionCall("trim")(expr)

  /**
    * Returns `str` with all characters changed to uppercase.
    * @example {{{ ucase(str) }}}
    */
  def ucase(expr: Expression): Expression = FunctionCall("ucase")(expr)

  /**
    * Converts the argument from a base 64 string `str` to a binary.
    * @example {{{ unbase64(str) }}}
    */
  def unbase64(expr: Expression): Expression = FunctionCall("unbase64")(expr)

  /**
    * Converts hexadecimal expr to binary.
    * @example {{{ unhex(expr) }}}
    */
  def unhex(expr: Expression): Expression = FunctionCall("unhex")(expr)

  /**
    * Returns str with all characters changed to uppercase.
    * @example {{{ upper(str) }}}
    */
  def upper(expr: Expression): Expression = FunctionCall("upper")(expr)

  /**
    * Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.
    * @example {{{ uuid() }}}
    */
  def uuid(): Expression = FunctionCall("uuid")()

  /**
    * Returns the population variance calculated from values of a group.
    * @example {{{ var_pop(expr) }}}
    */
  def var_pop(expr: Expression): Expression = FunctionCall("var_pop")(expr)

  /**
    * Returns the sample variance calculated from values of a group.
    * @example {{{ var_samp(expr) }}}
    */
  def var_samp(expr: Expression): Expression = FunctionCall("var_samp")(expr)

  /**
    * .
    * @example {{{ variance(expr) }}}
    */
  def variance(expr: Expression): Expression = FunctionCall("variance")(expr)

  /**
    * weekday(date) - Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
    * @example {{{ weekday(date) }}}
    */
  def weekday(expr: Expression): Expression = FunctionCall("weekday")(expr)

  /**
    * Returns the week of the year of the given date.
    * A week is considered to start on a Monday and week 1 is the first week with >3 days.
    * @example {{{ weekofyear(date) }}}
    */
  def weekofyear(expr: Expression): Expression = FunctionCall("weekofyear")(expr)

  /**
    * Returns a string array of values within the nodes of xml that match the XPath expression.
    * @example {{{ xpath(xml, xpath) }}}
    */
  def xpath(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath")(expr1, expr2)

  /**
    * Returns true if the XPath expression evaluates to true, or if a matching node is found.
    * @example {{{ xpath_boolean(xml, xpath) }}}
    */
  def xpath_boolean(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_boolean")(expr1, expr2)

  /**
    * Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    * @example {{{ xpath_double(xml, xpath) }}}
    */
  def xpath_double(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_double")(expr1, expr2)

  /**
    * Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    * @example {{{ xpath_float(xml, xpath) }}}
    */
  def xpath_float(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_float")(expr1, expr2)

  /**
    * Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    * @example {{{ xpath_int(xml, xpath) }}}
    */
  def xpath_int(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_int")(expr1, expr2)

  /**
    * Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    * @example {{{ xpath_long(xml, xpath) }}}
    */
  def xpath_long(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_long")(expr1, expr2)

  /**
    * Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    * @example {{{ xpath_number(xml, xpath) }}}
    */
  def xpath_number(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_number")(expr1, expr2)

  /**
    * Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    * @example {{{ xpath_short(xml, xpath) }}}
    */
  def xpath_short(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_short")(expr1, expr2)

  /**
    * Returns the text contents of the first xml node that matches the XPath expression.
    * @example {{{ xpath_string(xml, xpath) }}}
    */
  def xpath_string(expr1: Expression, expr2: Expression): Expression = FunctionCall("xpath_string")(expr1, expr2)

  /**
    * Returns the year component of the date/timestamp.
    * @example {{{ year(date) }}}
    */
  def year(expr: Expression): Expression = FunctionCall("year")(expr)

  /**
    * Merges the two given arrays, element-wise, into a single array using function. If one array is shorter,
    * nulls are appended at the end to match the length of the longer array, before applying function.
    * @example {{{ zip_with(left, right, func) }}}
    */
  def zip_with(expr1: Expression, expr2: Expression, expr3: Expression): Expression = FunctionCall("zip_with")(expr1, expr2, expr3)

}

/**
  * Native SQL Functions Singleton
  * @author lawrence.daniels@gmail.com
  */
object NativeFunctions extends NativeFunctions

