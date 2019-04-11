package com.qwery.models.expressions

/**
  * Native Function Model Facades
  * @author lawrence.daniels@gmail.com
  */
object NativeFunctions {

  /**
    * Abs - Returns the absolute value of the numeric value
    * @example {{{ abs(expr) }}}
    */
  object Abs {
    def apply(expr: Expression): FunctionCall = FunctionCall("abs")(expr)
  }

  /**
    * Acos - Returns the inverse cosine (a.k.a. arc cosine) of expr, as if computed by [[java.lang.Math#acos]]
    * @example {{{ acos(expr) }}}
    */
  object Acos {
    def apply(expr: Expression): FunctionCall = FunctionCall("acos")(expr)
  }

  /**
    * Add_Months - Returns the date that is num_months after start_date
    * @example {{{ add_months(start_date, num_months) }}}
    */
  object Add_Months {
    def apply(expr: Expression*): FunctionCall = FunctionCall("add_months")(expr: _*)
  }

  /**
    * Aggregate - Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state.
    * The final state is converted into the final result by applying a finish function
    * @example {{{ aggregate(expr1, ...) }}}
    */
  object Aggregate {
    def apply(expr: Expression*): FunctionCall = FunctionCall("aggregate")(expr: _*)
  }

  /**
    * Approx_Count_Distinct - Returns the estimated cardinality by HyperLogLog++. relativeSD defines the maximum estimation error allowed
    * @example {{{ approx_count_distinct(expr1, ...) }}}
    */
  object Approx_Count_Distinct {
    def apply(expr: Expression*): FunctionCall = FunctionCall("approx_count_distinct")(expr: _*)
  }

  /**
    * Approx_Percentile - Returns the approximate percentile value of numeric column col at the given percentage.
    * The value of percentage must be between 0.0 and 1.0. The  accuracy parameter (default: 10000)
    * is a positive numeric literal which controls approximation accuracy at the cost of memory.
    * Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the approximation.
    * When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
    * In this case, returns the approximate percentile array of column col at the given percentage array
    * @example {{{ approx_percentile(expr1, ...) }}}
    */
  object Approx_Percentile {
    def apply(expr: Expression*): FunctionCall = FunctionCall("approx_percentile")(expr: _*)
  }

  /**
    * Array - Returns an array with the given elements
    * @example {{{ array(expr1, ...) }}}
    */
  object Array {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array")(expr: _*)
  }

  /**
    * Array_Contains - Returns true if the array contains the value
    * @example {{{ array_contains(expr1, expr2) }}}
    */
  object Array_Contains {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_contains")(expr: _*)
  }

  /**
    * Array_Distinct - Removes duplicate values from the array
    * @example {{{ array_distinct(expr1, ...) }}}
    */
  object Array_Distinct {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_distinct")(expr: _*)
  }

  /**
    * Array_Except - Returns an array of the elements in array1 but not in array2, without duplicates.
    * @example {{{ array_except(expr1, expr2) }}}
    */
  object Array_Except {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_except")(expr: _*)
  }

  /**
    * Array_Index - Returns an array of the elements in the intersection of array1 and array2, without duplicates
    * @example {{{ array_index(expr1, expr2) }}}
    */
  object Array_Index {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_index")(expr: _*)
  }

  /**
    * Array_Intersect - Returns an array of the elements in the intersection of array1 and array2, without duplicates
    * @example {{{ array_intersect(array1, array2) }}}
    */
  object Array_Intersect {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_intersect")(expr: _*)
  }

  /**
    * Array_Join - Concatenates the elements of the given array using the delimiter and an optional string to
    * replace nulls. If no value is set for nullReplacement, any null value is filtered
    * @example {{{ array_join(array, delimiter[, nullReplacement]) }}}
    */
  object Array_Join {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_join")(expr: _*)
  }

  /**
    * Array_Max - Returns the maximum value in the array. NULL elements are skipped
    * @example {{{ array_max(array) }}}
    */
  object Array_Max {
    def apply(expr: Expression): FunctionCall = FunctionCall("array_max")(expr)
  }

  /**
    * Array_Min - Returns the minimum value in the array. NULL elements are skipped
    * @example {{{ array_min(array) }}}
    */
  object Array_Min {
    def apply(expr: Expression): FunctionCall = FunctionCall("array_min")(expr)
  }

  /**
    * Array_Overlap - Returns true if a1 contains at least a non-null element present also in a2. If the arrays have no
    * common element and they are both non-empty and either of them contains a null element null is returned,
    * false otherwise
    * @example {{{ arrays_overlap(array1, array2) }}}
    */
  object Array_Overlap {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_overlap")(expr: _*)
  }

  /**
    * Array_Position - Returns the (1-based) index of the first element of the array as long
    * @example {{{ array_position(array, element) }}}
    */
  object Array_Position {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_position")(expr: _*)
  }

  /**
    * Array_Remove - Remove all elements that equal to element from array
    * @example {{{ array_remove(array, element) }}}
    */
  object Array_Remove {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_remove")(expr: _*)
  }

  /**
    * Array_Repeat - Returns the array containing element count times
    * @example {{{ array_repeat(element, count) }}}
    */
  object Array_Repeat {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_repeat")(expr: _*)
  }

  /**
    * Array_Sort - Sorts the input array in ascending order. The elements of the input array must be orderable.
    * Null elements will be placed at the end of the returned array
    * @example {{{ array_sort(array) }}}
    */
  object Array_Sort {
    def apply(expr: Expression): FunctionCall = FunctionCall("array_sort")(expr)
  }

  /**
    * Array_Union - Returns an array of the elements in the union of array1 and array2, without duplicates
    * @example {{{ array_union(array1, array2) }}}
    */
  object Array_Union {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_union")(expr: _*)
  }

  /**
    * Arrays_Zip - Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays
    * @example {{{ arrays_zip(array1, array2, ...) }}}
    */
  object Arrays_Zip {
    def apply(expr: Expression*): FunctionCall = FunctionCall("arrays_zip")(expr: _*)
  }

  /**
    * Ascii - Returns the numeric value of the first character of string
    * @example {{{ ascii(expr) }}}
    */
  object Ascii {
    def apply(expr: Expression): FunctionCall = FunctionCall("ascii")(expr)
  }

  /**
    * Asin - Returns the inverse sine (a.k.a. arc sine) the arc sin of expr, as if computed by [[java.lang.Math#asin]]
    * @example {{{ asin(expr) }}}
    */
  object Asin {
    def apply(expr: Expression): FunctionCall = FunctionCall("asin")(expr)
  }

  /**
    * Assert_True - Throws an exception if expr is not true
    * @example {{{ assert_true(expr) }}}
    */
  object Assert_True {
    def apply(expr: Expression): FunctionCall = FunctionCall("assert_true")(expr)
  }

  /**
    * Atan - Returns the inverse tangent (a.k.a. arc tangent) of expr, as if computed by [[java.lang.Math#atan]]
    * @example {{{ atan(expr) }}}
    */
  object Atan {
    def apply(expr: Expression): FunctionCall = FunctionCall("atan")(expr)
  }

  /**
    * Atan2 - Returns the angle in radians between the positive x-axis of a plane and the point given by the
    * coordinates (exprX, exprY), as if computed by [[java.lang.Math#atan2]]
    * @example {{{ atan2(expr1, expr2) }}}
    */
  object Atan2 {
    def apply(expr: Expression*): FunctionCall = FunctionCall("atan2")(expr: _*)
  }

  /**
    * Avg - Returns the mean calculated from values of a group
    * @example {{{ avg(expr) }}}
    */
  object Avg {
    def apply(expr: Expression): FunctionCall = FunctionCall("avg")(expr)
  }

  /**
    * Base64 - Converts the argument from a binary bin to a base 64 string
    * @example {{{ base64(expr) }}}
    */
  object Base64 {
    def apply(expr: Expression): FunctionCall = FunctionCall("base64")(expr)
  }

  /**
    * Bigint - Casts the value expr to the target data type bigint
    * @example {{{ bigint(expr) }}}
    */
  object Bigint {
    def apply(expr: Expression): FunctionCall = FunctionCall("bigint")(expr)
  }

  /**
    * Bin - Returns the string representation of the long value expr represented in binary
    * @example {{{ bin(expr) }}}
    */
  object Bin {
    def apply(expr: Expression): FunctionCall = FunctionCall("bin")(expr)
  }

  /**
    * Binary - Casts the value expr to the target data type binary
    * @example {{{ binary(expr) }}}
    */
  object Binary {
    def apply(expr: Expression): FunctionCall = FunctionCall("binary")(expr)
  }

  /**
    * Bit_Length - Returns the bit length of string data or number of bits of binary data
    * @example {{{ bit_length(expr) }}}
    */
  object Bit_Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("bit_length")(expr)
  }

  /**
    * Boolean - Casts the value expr to the target data type boolean
    * @example {{{ boolean(expr) }}}
    */
  object Boolean {
    def apply(expr: Expression): FunctionCall = FunctionCall("boolean")(expr)
  }

  /**
    * Bround - Returns expr rounded to d decimal places using HALF_EVEN rounding mode
    * @example {{{ bround(expr, d) }}}
    */
  object Bround {
    def apply(expr: Expression*): FunctionCall = FunctionCall("bround")(expr: _*)
  }

  /**
    * Cardinality - Returns the size of an array or a map. The function returns -1 if its input is null and
    *spark.sql.legacy.sizeOfNull is set to true. If spark.sql.legacy.sizeOfNull is set to false,
    * the function returns null for null input. By default, the spark.sql.legacy.sizeOfNull parameter
    * is set to true
    * @example {{{ cardinality(expr) }}}
    */
  object Cardinality {
    def apply(expr: Expression): FunctionCall = FunctionCall("cardinality")(expr)
  }

  /**
    * Cbrt - Returns the cube root of expr
    * @example {{{ cbrt(expr) }}}
    */
  object Cbrt {
    def apply(expr: Expression): FunctionCall = FunctionCall("cbrt")(expr)
  }

  /**
    * Ceil - Returns the smallest integer not smaller than expr
    * @example {{{ ceil(expr) }}}
    */
  object Ceil {
    def apply(expr: Expression): FunctionCall = FunctionCall("ceil")(expr)
  }

  /**
    * Ceiling - Returns the smallest integer not smaller than expr
    * @example {{{ ceiling(expr) }}}
    */
  object Ceiling {
    def apply(expr: Expression): FunctionCall = FunctionCall("ceiling")(expr)
  }

  /**
    * Char - Returns the ASCII character having the binary equivalent to expr.
    * If n is larger than 256 the result is equivalent to chr(n % 256)
    * @example {{{ char(expr) }}}
    */
  object Char {
    def apply(expr: Expression): FunctionCall = FunctionCall("char")(expr)
  }

  /**
    * Char_Length - Returns the character length of string data or number of bytes of binary data. The length of
    * string data includes the trailing spaces. The length of binary data includes binary zeros
    * @example {{{ char_length(expr) }}}
    */
  object Char_Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("char_length")(expr)
  }

  /**
    * Character_Length - Collects and returns a list of non-unique elements
    * @example {{{ character_length(expr) }}}
    */
  object Character_Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("character_length")(expr)
  }

  /**
    * Chr - Returns the ASCII character having the binary equivalent to expr.
    * If n is larger than 256 the result is equivalent to chr(n % 256)
    * @example {{{ chr(expr) }}}
    */
  object Chr {
    def apply(expr: Expression): FunctionCall = FunctionCall("chr")(expr)
  }

  /**
    * Coalesce - Returns the first non-null argument if exists. Otherwise, null
    * @example {{{ coalesce(expr1, ...) }}}
    */
  object Coalesce {
    def apply(expr: Expression*): FunctionCall = FunctionCall("coalesce")(expr: _*)
  }

  /**
    * Collect_List - Collects and returns a list of non-unique elements
    * @example {{{ collect_list(expr) }}}
    */
  object Collect_List {
    def apply(expr: Expression): FunctionCall = FunctionCall("collect_list")(expr)
  }

  /**
    * Collect_Set - Collects and returns a set of unique elements
    * @example {{{ collect_set(expr) }}}
    */
  object Collect_Set {
    def apply(expr: Expression): FunctionCall = FunctionCall("collect_set")(expr)
  }

  /**
    * Concat - Returns the concatenation of col1, col2, ..., colN
    * @example {{{ concat(col1, col2, ..., colN) }}}
    */
  object Concat {
    def apply(expr: Expression*): FunctionCall = FunctionCall("concat")(expr: _*)
  }

  /**
    * Concat_Ws - Returns the concatenation of the strings separated by sep
    * @example {{{ concat_ws(sep, [str | array(str)]+) }}}
    */
  object Concat_Ws {
    def apply(expr: Expression*): FunctionCall = FunctionCall("concat_ws")(expr: _*)
  }

  /**
    * Conv - Convert num from from_base to to_base.
    * @example {{{ conv(num, from_base, to_base) }}}
    */
  object Conv {
    def apply(expr: Expression*): FunctionCall = FunctionCall("conv")(expr: _*)
  }

  /**
    * Corr - Returns Pearson coefficient of correlation between a set of number pairs
    * @example {{{ corr(expr1, expr2) }}}
    */
  object Corr {
    def apply(expr: Expression*): FunctionCall = FunctionCall("corr")(expr: _*)
  }

  /**
    * Cos - Returns the cosine of expr, as if computed by [[java.lang.Math#cos]]
    * @example {{{ cos(expr) }}}
    */
  object Cos {
    def apply(expr: Expression): FunctionCall = FunctionCall("cos")(expr)
  }

  /**
    * Cosh - Returns the hyperbolic cosine of expr, as if computed by [[java.lang.Math#cosh]]
    * @example {{{ cosh(expr) }}}
    */
  object Cosh {
    def apply(expr: Expression): FunctionCall = FunctionCall("cosh")(expr)
  }

  /**
    * Cot - Returns the cotangent of expr, as if computed by [[java.lang.Math#cot]]
    * @example {{{ cot(expr) }}}
    */
  object Cot {
    def apply(expr: Expression): FunctionCall = FunctionCall("cot")(expr)
  }

  /**
    * Count - Returns the total number of retrieved rows, including rows containing null
    * @example {{{ count(expr1, ...) }}}
    */
  object Count {
    def apply(expr: Expression*): FunctionCall = FunctionCall("count")(expr: _*)
  }

  /**
    * Count_Min_Sketch - Returns a count-min sketch of a column with the given esp, confidence and seed.
    * The result is an array of bytes, which can be deserialized to a CountMinSketch before usage.
    * Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space
    * @example {{{ count_min_sketch(col, eps, confidence, seed) }}}
    */
  object Count_Min_Sketch {
    def apply(expr: Expression*): FunctionCall = FunctionCall("count_min_sketch")(expr: _*)
  }

  /**
    * Covar_Pop - Returns the population covariance of a set of number pairs
    * @example {{{ covar_pop(expr1, expr2) }}}
    */
  object Covar_Pop {
    def apply(expr: Expression*): FunctionCall = FunctionCall("covar_pop")(expr: _*)
  }

  /**
    * Covar_Samp - Returns the sample covariance of a set of number pairs
    * @example {{{ covar_samp(expr1, expr2) }}}
    */
  object Covar_Samp {
    def apply(expr: Expression*): FunctionCall = FunctionCall("covar_samp")(expr: _*)
  }

  /**
    * Crc32 - Returns a cyclic redundancy check value of the expr as a bigint
    * @example {{{ crc32(expr) }}}
    */
  object Crc32 {
    def apply(expr: Expression): FunctionCall = FunctionCall("crc32")(expr)
  }

  /**
    * Cube - [[https://spark.apache.org/docs/2.4.0/api/sql/index.html#cube]]
    * @example {{{ cube(expr1, ...) }}}
    */
  object Cube {
    def apply(expr: Expression*): FunctionCall = FunctionCall("cube")(expr: _*)
  }

  /**
    * Cume_Dist - Computes the position of a value relative to all values in the partition
    * @example {{{ cume_dist() }}}
    */
  object Cume_Dist {
    def apply(expr: Expression*): FunctionCall = FunctionCall("cume_dist")(expr: _*)
  }

  /**
    * Current_Database - Returns the current database
    * @example {{{ current_database() }}}
    */
  object Current_Database {
    def apply(expr: Expression*): FunctionCall = FunctionCall("current_database")(expr: _*)
  }

  /**
    * Current_Date - Returns the current date at the start of query evaluation
    * @example {{{ current_date() }}}
    */
  object Current_Date {
    def apply(expr: Expression*): FunctionCall = FunctionCall("current_date")(expr: _*)
  }

  /**
    * Current_Timestamp - Returns the current timestamp at the start of query evaluation
    * @example {{{ current_timestamp() }}}
    */
  object Current_Timestamp {
    def apply(expr: Expression*): FunctionCall = FunctionCall("current_timestamp")(expr: _*)
  }

  /**
    * Date - Casts the value expr to the target data type date
    * @example {{{ date(expr) }}}
    */
  object Date {
    def apply(expr: Expression): FunctionCall = FunctionCall("date")(expr)
  }

  /**
    * Date_Add - Returns the date that is num_days after start_date
    * @example {{{ date_add(start_date, num_days) }}}
    */
  object Date_Add {
    def apply(expr: Expression*): FunctionCall = FunctionCall("date_add")(expr: _*)
  }

  /**
    * Date_Format - Converts timestamp to a value of string in the format specified by the date format fmt
    * @example {{{ date_format(timestamp, fmt) }}}
    */
  object Date_Format {
    def apply(expr: Expression*): FunctionCall = FunctionCall("date_format")(expr: _*)
  }

  /**
    * Date_Sub - Returns the date that is num_days before start_date
    * @example {{{ date_sub(start_date, num_days) }}}
    */
  object Date_Sub {
    def apply(expr: Expression*): FunctionCall = FunctionCall("date_sub")(expr: _*)
  }

  /**
    * Date_Trunc - Returns timestamp ts truncated to the unit specified by the format model fmt.
    * fmt should be one of ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD",
    * "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"]
    * @example {{{ date_trunc(expr1, expr2) }}}
    */
  object Date_Trunc {
    def apply(expr: Expression*): FunctionCall = FunctionCall("date_trunc")(expr: _*)
  }

  /**
    * Datediff - Returns the number of days from startDate to endDate.
    * @example {{{ datediff(endDate, startDate) }}}
    */
  object Datediff {
    def apply(expr: Expression*): FunctionCall = FunctionCall("datediff")(expr: _*)
  }

  /**
    * Day - Returns the day of month of the date/timestamp
    * @example {{{ day(date) }}}
    */
  object Day {
    def apply(expr: Expression): FunctionCall = FunctionCall("day")(expr)
  }

  /**
    * Dayofmonth - Returns the day of month of the date/timestamp
    * @example {{{ dayofmonth(date) }}}
    */
  object Dayofmonth {
    def apply(expr: Expression): FunctionCall = FunctionCall("dayofmonth")(expr)
  }

  /**
    * Dayofweek - Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
    * @example {{{ dayofweek(date) }}}
    */
  object Dayofweek {
    def apply(expr: Expression): FunctionCall = FunctionCall("dayofweek")(expr)
  }

  /**
    * Decimal - Casts the value expr to the target data type decimal
    * @example {{{ decimal(expr) }}}
    */
  object Decimal {
    def apply(expr: Expression): FunctionCall = FunctionCall("decimal")(expr)
  }

  /**
    * Decode - Decodes the first argument using the second argument character set
    * @example {{{ decode(bin, charset) }}}
    */
  object Decode {
    def apply(expr: Expression*): FunctionCall = FunctionCall("decode")(expr: _*)
  }

  /**
    * Degrees - Converts radians to degrees
    * @example {{{ degrees(expr) }}}
    */
  object Degrees {
    def apply(expr: Expression): FunctionCall = FunctionCall("degrees")(expr)
  }

  /**
    * Dense_Rank - Computes the rank of a value in a group of values. The result is one plus the previously assigned rank value.
    * Unlike the function rank, dense_rank will not produce gaps in the ranking sequence
    * @example {{{ dense_rank() }}}
    */
  object Dense_Rank {
    def apply(expr: Expression*): FunctionCall = FunctionCall("dense_rank")(expr: _*)
  }

  /**
    * Distinct -
    * @example {{{ distinct(expr) }}}
    */
  object Distinct {
    def apply(expr: Expression): FunctionCall = FunctionCall("distinct")(expr)
  }

  /**
    * Double - Casts the value expr to the target data type double
    * @example {{{ double(expr) }}}
    */
  object Double {
    def apply(expr: Expression): FunctionCall = FunctionCall("double")(expr)
  }

  /**
    * E - Returns Euler's number, e
    * @example {{{ e() }}}
    */
  object E {
    def apply(expr: Expression*): FunctionCall = FunctionCall("e")(expr: _*)
  }

  /**
    * Elt - Returns the n-th input, e.g., returns input2 when n is 2
    * @example {{{ elt(n, input1, input2, ...) }}}
    */
  object Elt {
    def apply(expr: Expression*): FunctionCall = FunctionCall("elt")(expr: _*)
  }

  /**
    * Encode - Encodes the first argument using the second argument character set
    * @example {{{ encode(str, charset) }}}
    */
  object Encode {
    def apply(expr: Expression*): FunctionCall = FunctionCall("encode")(expr: _*)
  }

  /**
    * Exp - Returns e to the power of expr
    * @example {{{ exp(expr) }}}
    */
  object Exp {
    def apply(expr: Expression): FunctionCall = FunctionCall("exp")(expr)
  }

  /**
    * Explode - Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns
    * @example {{{ explode(expr) }}}
    */
  object Explode {
    def apply(expr: Expression): FunctionCall = FunctionCall("explode")(expr)
  }

  /**
    * Explode_Outer - Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns
    * @example {{{ explode_outer(expr) }}}
    */
  object Explode_Outer {
    def apply(expr: Expression): FunctionCall = FunctionCall("explode_outer")(expr)
  }

  /**
    * Expm1 - Returns exp(expr) - 1
    * @example {{{ expm1(expr) }}}
    */
  object Expm1 {
    def apply(expr: Expression): FunctionCall = FunctionCall("expm1")(expr)
  }

  /**
    * Factorial - Returns the factorial of expr. expr is [0..20]. Otherwise, null
    * @example {{{ factorial(expr) }}}
    */
  object Factorial {
    def apply(expr: Expression): FunctionCall = FunctionCall("factorial")(expr)
  }

  /**
    * Find_In_Set - Returns the index (1-based) of the given string (str) in the comma-delimited list (str_array).
    * Returns 0, if the string was not found or if the given string (str) contains a comma
    * @example {{{ find_in_set(expr1, expr2) }}}
    */
  object Find_In_Set {
    def apply(expr: Expression*): FunctionCall = FunctionCall("find_in_set")(expr: _*)
  }

  /**
    * First - Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values
    * @example {{{ first(expr[, isIgnoreNull]) }}}
    */
  object First {
    def apply(expr: Expression*): FunctionCall = FunctionCall("first")(expr: _*)
  }

  /**
    * First_Value - Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values
    * @example {{{ first_value(expr[, isIgnoreNull]) }}}
    */
  object First_Value {
    def apply(expr: Expression*): FunctionCall = FunctionCall("first_value")(expr: _*)
  }

  /**
    * Flatten - Transforms an array of arrays into a single array
    * @example {{{ flatten(arrayOfArrays) }}}
    */
  object Flatten {
    def apply(expr: Expression): FunctionCall = FunctionCall("flatten")(expr)
  }

  /**
    * Float - Casts the value expr to the target data type float
    * @example {{{ float(expr) }}}
    */
  object Float {
    def apply(expr: Expression): FunctionCall = FunctionCall("float")(expr)
  }

  /**
    * Floor - Returns the largest integer not greater than expr
    * @example {{{ floor(expr) }}}
    */
  object Floor {
    def apply(expr: Expression): FunctionCall = FunctionCall("floor")(expr)
  }

  /**
    * Format_Number - Formats the number expr1 like '#,###,###.##', rounded to expr2 decimal places.
    * If expr2 is 0, the result has no decimal point or fractional part. expr2 also accept a user specified format.
    * This is supposed to function like MySQL's FORMAT.
    * @example {{{ format_number(expr1, expr2) }}}
    */
  object Format_Number {
    def apply(expr: Expression*): FunctionCall = FunctionCall("format_number")(expr: _*)
  }

  /**
    * Format_String - Returns a formatted string from printf-style format strings
    * @example {{{ format_string(strfmt, obj, ...) }}}
    */
  object Format_String {
    def apply(expr: Expression*): FunctionCall = FunctionCall("format_string")(expr: _*)
  }

  /**
    * From_Json - Returns a struct value with the given jsonStr and schema
    * @example {{{ from_json(jsonStr, schema[, options]) }}}
    */
  object From_Json {
    def apply(expr: Expression*): FunctionCall = FunctionCall("from_json")(expr: _*)
  }

  /**
    * From_Unixtime - Returns unix_time in the specified format
    * @example {{{ from_unixtime(unix_time, format) }}}
    */
  object From_Unixtime {
    def apply(expr: Expression*): FunctionCall = FunctionCall("from_unixtime")(expr: _*)
  }

  /**
    * From_Utc_Timestamp - Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC,
    * and renders that time as a timestamp in the given time zone.
    * For example, 'GMT+1' would yield '2017-07-14 03:40:00.0'
    * @example {{{ from_utc_timestamp(timestamp, timezone) }}}
    */
  object From_Utc_Timestamp {
    def apply(expr: Expression*): FunctionCall = FunctionCall("from_utc_timestamp")(expr: _*)
  }

  /**
    * Get_Json_Object - Extracts a json object from path
    * @example {{{ get_json_object(json_txt, path) }}}
    */
  object Get_Json_Object {
    def apply(expr: Expression*): FunctionCall = FunctionCall("get_json_object")(expr: _*)
  }

  /**
    * Greatest - Returns the greatest value of all parameters, skipping null values
    * @example {{{ greatest(expr1, ...) }}}
    */
  object Greatest {
    def apply(expr: Expression*): FunctionCall = FunctionCall("greatest")(expr: _*)
  }

  /**
    * Hash - Returns a hash value of the arguments
    * @example {{{ hash(expr1, ...) }}}
    */
  object Hash {
    def apply(expr: Expression*): FunctionCall = FunctionCall("hash")(expr: _*)
  }

  /**
    * Hex - Converts expr to hexadecimal
    * @example {{{ hex(expr) }}}
    */
  object Hex {
    def apply(expr: Expression): FunctionCall = FunctionCall("hex")(expr)
  }

  /**
    * Hour - Returns the hour component of the string/timestamp
    * @example {{{ hour(timestamp) }}}
    */
  object Hour {
    def apply(expr: Expression): FunctionCall = FunctionCall("hour")(expr)
  }

  /**
    * Hypot - Returns sqrt(expr1^2 + expr2^2)
    * @example {{{ hypot(expr1, expr2) }}}
    */
  object Hypot {
    def apply(expr: Expression*): FunctionCall = FunctionCall("hypot")(expr: _*)
  }

  /**
    * In - Returns true if expr equals to any valN
    * @example {{{ in(expr1, ...) }}}
    */
  object In {
    def apply(expr: Expression*): FunctionCall = FunctionCall("in")(expr: _*)
  }

  /**
    * Initcap - Returns str with the first letter of each word in uppercase. All other letters are in lowercase.
    * Words are delimited by white space
    * @example {{{ initcap(str) }}}
    */
  object Initcap {
    def apply(expr: Expression): FunctionCall = FunctionCall("initcap")(expr)
  }

  /**
    * Inline - Explodes an array of structs into a table
    * @example {{{ inline(expr) }}}
    */
  object Inline {
    def apply(expr: Expression): FunctionCall = FunctionCall("inline")(expr)
  }

  /**
    * Inline_Outer - Explodes an array of structs into a table
    * @example {{{ inline_outer(expr) }}}
    */
  object Inline_Outer {
    def apply(expr: Expression): FunctionCall = FunctionCall("inline_outer")(expr)
  }

  /**
    * Input_File_Block_Length - Returns the length of the block being read, or -1 if not available
    * @example {{{ input_file_block_length() }}}
    */
  object Input_File_Block_Length {
    def apply(expr: Expression*): FunctionCall = FunctionCall("input_file_block_length")(expr: _*)
  }

  /**
    * Input_File_Block_Start - Returns the start offset of the block being read, or -1 if not available
    * @example {{{ input_file_block_start() }}}
    */
  object Input_File_Block_Start {
    def apply(expr: Expression*): FunctionCall = FunctionCall("input_file_block_start")(expr: _*)
  }

  /**
    * Input_File_Name - Returns the name of the file being read, or empty string if not available
    * @example {{{ input_file_name() }}}
    */
  object Input_File_Name {
    def apply(expr: Expression*): FunctionCall = FunctionCall("input_file_name")(expr: _*)
  }

  /**
    * Instr - Returns the (1-based) index of the first occurrence of substr in str
    * @example {{{ instr(str, substr) }}}
    */
  object Instr {
    def apply(expr: Expression*): FunctionCall = FunctionCall("instr")(expr: _*)
  }

  /**
    * Int - Casts the value expr to the target data type int
    * @example {{{ int(expr) }}}
    */
  object Int {
    def apply(expr: Expression): FunctionCall = FunctionCall("int")(expr)
  }

  /**
    * Isnan - Returns true if expr is NaN, or false otherwise
    * @example {{{ isnan(expr) }}}
    */
  object Isnan {
    def apply(expr: Expression): FunctionCall = FunctionCall("isnan")(expr)
  }

  /**
    * Isnotnull - Returns true if expr is not null, or false otherwise
    * @example {{{ isnotnull(expr) }}}
    */
  object Isnotnull {
    def apply(expr: Expression): FunctionCall = FunctionCall("isnotnull")(expr)
  }

  /**
    * Isnull - Returns true if expr is null, or false otherwise
    * @example {{{ isnull(expr) }}}
    */
  object Isnull {
    def apply(expr: Expression): FunctionCall = FunctionCall("isnull")(expr)
  }

  /**
    * Java_Method - Calls a method with reflection
    * @example {{{ java_method(class, method[, arg1[, arg2 ..]]) }}}
    */
  object Java_Method {
    def apply(expr: Expression*): FunctionCall = FunctionCall("java_method")(expr: _*)
  }

  /**
    * Json_Tuple - Returns a tuple like the function get_json_object, but it takes multiple names.
    * All the input parameters and output column types are string
    * @example {{{ json_tuple(jsonStr, p1, p2, ..., pn) }}}
    */
  object Json_Tuple {
    def apply(expr: Expression*): FunctionCall = FunctionCall("json_tuple")(expr: _*)
  }

  /**
    * Kurtosis - Returns the kurtosis value calculated from values of a group
    * @example {{{ kurtosis(expr) }}}
    */
  object Kurtosis {
    def apply(expr: Expression): FunctionCall = FunctionCall("kurtosis")(expr)
  }

  /**
    * Lag - Returns the value of input at the offsetth row before the current row in the window. The default value of
    * offset is 1 and the default value of default is null. If the value of  input at the offsetth row is null,
    * null is returned. If there is no such offset row (e.g., when the offset is 1, the first row of the window
    * does not have any previous row), default is returned
    * @example {{{ lag(input[, offset[, default]]) }}}
    */
  object Lag {
    def apply(expr: Expression*): FunctionCall = FunctionCall("lag")(expr: _*)
  }

  /**
    * Last - Returns the last value of expr for a group of rows. If isIgnoreNull is true,
    * returns only non-null values.
    * @example {{{ last(expr[, isIgnoreNull]) }}}
    */
  object Last {
    def apply(expr: Expression*): FunctionCall = FunctionCall("last")(expr: _*)
  }

  /**
    * Last_Day - Returns the last day of the month which the date belongs to
    * @example {{{ last_day(date) }}}
    */
  object Last_Day {
    def apply(expr: Expression): FunctionCall = FunctionCall("last_day")(expr)
  }

  /**
    * Last_Value - Returns the last value of expr for a group of rows. If isIgnoreNull is true,
    * returns only non-null values.
    * @example {{{ last_value(expr[, isIgnoreNull]) }}}
    */
  object Last_Value {
    def apply(expr: Expression*): FunctionCall = FunctionCall("last_value")(expr: _*)
  }

  /**
    * Lcase - Returns str with all characters changed to lowercase
    * @example {{{ lcase(str) }}}
    */
  object Lcase {
    def apply(expr: Expression): FunctionCall = FunctionCall("lcase")(expr)
  }

  /**
    * Length -
    * @example {{{ length(expr) }}}
    */
  object Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("length")(expr)
  }

  /**
    * Lower -
    * @example {{{ lower(expr) }}}
    */
  object Lower {
    def apply(expr: Expression): FunctionCall = FunctionCall("lower")(expr)
  }

  /**
    * Lpad -
    * @example {{{ lpad(expr1, expr2, expr3) }}}
    */
  object Lpad {
    def apply(expr: Expression*): FunctionCall = FunctionCall("lpad")(expr: _*)
  }

  /**
    * Ltrim -
    * @example {{{ ltrim(expr) }}}
    */
  object Ltrim {
    def apply(expr: Expression): FunctionCall = FunctionCall("ltrim")(expr)
  }

  /**
    * Max -
    * @example {{{ max(expr) }}}
    */
  object Max {
    def apply(expr: Expression): FunctionCall = FunctionCall("max")(expr)
  }

  /**
    * Mean -
    * @example {{{ mean(expr) }}}
    */
  object Mean {
    def apply(expr: Expression): FunctionCall = FunctionCall("mean")(expr)
  }

  /**
    * Min -
    * @example {{{ min(expr) }}}
    */
  object Min {
    def apply(expr: Expression): FunctionCall = FunctionCall("min")(expr)
  }

  /**
    * Rpad -
    * @example {{{ rpad(expr1, expr2, expr3) }}}
    */
  object Rpad {
    def apply(expr: Expression*): FunctionCall = FunctionCall("rpad")(expr: _*)
  }

  /**
    * Rtrim -
    * @example {{{ rtrim(expr) }}}
    */
  object Rtrim {
    def apply(expr: Expression): FunctionCall = FunctionCall("rtrim")(expr)
  }

  /**
    * Split -
    * @example {{{ split(expr1, expr2) }}}
    */
  object Split {
    def apply(expr: Expression*): FunctionCall = FunctionCall("split")(expr: _*)
  }

  /**
    * Stddev -
    * @example {{{ stddev(expr) }}}
    */
  object Stddev {
    def apply(expr: Expression): FunctionCall = FunctionCall("stddev")(expr)
  }

  /**
    * Substr -
    * @example {{{ substr(expr1, expr2, expr3) }}}
    */
  object Substr {
    def apply(expr: Expression*): FunctionCall = FunctionCall("substr")(expr: _*)
  }

  /**
    * Substring -
    * @example {{{ substring(expr1, expr2, expr3) }}}
    */
  object Substring {
    def apply(expr: Expression*): FunctionCall = FunctionCall("substring")(expr: _*)
  }

  /**
    * Sum -
    * @example {{{ sum(expr) }}}
    */
  object Sum {
    def apply(expr: Expression): FunctionCall = FunctionCall("sum")(expr)
  }

  /**
    * To_Date -
    * @example {{{ to_date(expr) }}}
    */
  object To_Date {
    def apply(expr: Expression): FunctionCall = FunctionCall("to_date")(expr)
  }

  /**
    * Trim -
    * @example {{{ trim(expr) }}}
    */
  object Trim {
    def apply(expr: Expression): FunctionCall = FunctionCall("trim")(expr)
  }

  /**
    * Ucase - Returns str with all characters changed to uppercase
    * @example {{{ ucase(str) }}}
    */
  object Ucase {
    def apply(expr: Expression): FunctionCall = FunctionCall("ucase")(expr)
  }

  /**
    * Unbase64 - Converts the argument from a base 64 string str to a binary
    * @example {{{ unbase64(str) }}}
    */
  object Unbase64 {
    def apply(expr: Expression): FunctionCall = FunctionCall("unbase64")(expr)
  }

  /**
    * Unhex - Converts hexadecimal expr to binary
    * @example {{{ unhex(expr) }}}
    */
  object Unhex {
    def apply(expr: Expression): FunctionCall = FunctionCall("unhex")(expr)
  }

  /**
    * Upper -
    * @example {{{ upper(expr) }}}
    */
  object Upper {
    def apply(expr: Expression): FunctionCall = FunctionCall("upper")(expr)
  }

  /**
    * Variance -
    * @example {{{ variance(expr) }}}
    */
  object Variance {
    def apply(expr: Expression): FunctionCall = FunctionCall("variance")(expr)
  }

  /**
    * Weekofyear -
    * @example {{{ weekofyear(expr) }}}
    */
  object Weekofyear {
    def apply(expr: Expression): FunctionCall = FunctionCall("weekofyear")(expr)
  }

  /**
    * Year -
    * @example {{{ year(expr) }}}
    */
  object Year {
    def apply(expr: Expression): FunctionCall = FunctionCall("year")(expr)
  }

}
