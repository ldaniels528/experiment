package com.qwery.models.expressions

/**
  * Native SQL Functions Model Facades
  * @author lawrence.daniels@gmail.com
  * @note This is a generated class.
  */
object NativeFunctions {

  /**
    * Returns the absolute value of the numeric value.
    * @example {{{ abs(expr) }}}
    */
  object Abs {
    def apply(expr: Expression): FunctionCall = FunctionCall("abs")(expr)
  }

  /**
    * Returns the inverse cosine (a.k.a. arc cosine) of expr, as if computed by [[java.lang.Math#acos]].
    * @example {{{ acos(expr) }}}
    */
  object Acos {
    def apply(expr: Expression): FunctionCall = FunctionCall("acos")(expr)
  }

  /**
    * Returns the date that is num_months after start_date.
    * @example {{{ add_months(start_date, num_months) }}}
    */
  object Add_Months {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("add_months")(expr1, expr2)
  }

  /**
    * Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state.
    * The final state is converted into the final result by applying a finish function.
    * @example {{{ aggregate(expr1, ...) }}}
    */
  object Aggregate {
    def apply(expr: Expression*): FunctionCall = FunctionCall("aggregate")(expr: _*)
  }

  /**
    * Returns the estimated cardinality by HyperLogLog++. relativeSD defines the maximum estimation error allowed.
    * @example {{{ approx_count_distinct(expr1, ...) }}}
    */
  object Approx_Count_Distinct {
    def apply(expr: Expression*): FunctionCall = FunctionCall("approx_count_distinct")(expr: _*)
  }

  /**
    * Returns the approximate percentile value of numeric column col at the given percentage.
    * The value of percentage must be between 0.0 and 1.0. The  accuracy parameter (default: 10000)
    * is a positive numeric literal which controls approximation accuracy at the cost of memory.
    * Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the approximation.
    * When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
    * In this case, returns the approximate percentile array of column col at the given percentage array.
    * @example {{{ approx_percentile(expr1, ...) }}}
    */
  object Approx_Percentile {
    def apply(expr: Expression*): FunctionCall = FunctionCall("approx_percentile")(expr: _*)
  }

  /**
    * Returns an array with the given elements.
    * @example {{{ array(expr1, ...) }}}
    */
  object Array {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array")(expr: _*)
  }

  /**
    * Returns true if the array contains the value.
    * @example {{{ array_contains(expr1, expr2) }}}
    */
  object Array_Contains {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_contains")(expr1, expr2)
  }

  /**
    * Removes duplicate values from the array.
    * @example {{{ array_distinct(expr1, ...) }}}
    */
  object Array_Distinct {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_distinct")(expr: _*)
  }

  /**
    * Returns an array of the elements in array1 but not in array2, without duplicates.
    * @example {{{ array_except(expr1, expr2) }}}
    */
  object Array_Except {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_except")(expr1, expr2)
  }

  /**
    * Returns an array of the elements in the intersection of array1 and array2, without duplicates.
    * @example {{{ array_index(expr1, expr2) }}}
    */
  object Array_Index {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_index")(expr1, expr2)
  }

  /**
    * Returns an array of the elements in the intersection of array1 and array2, without duplicates.
    * @example {{{ array_intersect(array1, array2) }}}
    */
  object Array_Intersect {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_intersect")(expr1, expr2)
  }

  /**
    * Concatenates the elements of the given array using the delimiter and an optional string to
    * replace nulls. If no value is set for nullReplacement, any null value is filtered.
    * @example {{{ array_join(array, delimiter[, nullReplacement]) }}}
    */
  object Array_Join {
    def apply(expr: Expression*): FunctionCall = FunctionCall("array_join")(expr: _*)
  }

  /**
    * Returns the maximum value in the array. NULL elements are skipped.
    * @example {{{ array_max(array) }}}
    */
  object Array_Max {
    def apply(expr: Expression): FunctionCall = FunctionCall("array_max")(expr)
  }

  /**
    * Returns the minimum value in the array. NULL elements are skipped.
    * @example {{{ array_min(array) }}}
    */
  object Array_Min {
    def apply(expr: Expression): FunctionCall = FunctionCall("array_min")(expr)
  }

  /**
    * Returns true if a1 contains at least a non-null element present also in a2. If the arrays have no
    * common element and they are both non-empty and either of them contains a null element null is returned,
    * false otherwise.
    * @example {{{ arrays_overlap(array1, array2) }}}
    */
  object Array_Overlap {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_overlap")(expr1, expr2)
  }

  /**
    * Returns the (1-based) index of the first element of the array as long.
    * @example {{{ array_position(array, element) }}}
    */
  object Array_Position {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_position")(expr1, expr2)
  }

  /**
    * Remove all elements that equal to element from array.
    * @example {{{ array_remove(array, element) }}}
    */
  object Array_Remove {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_remove")(expr1, expr2)
  }

  /**
    * Returns the array containing element count times.
    * @example {{{ array_repeat(element, count) }}}
    */
  object Array_Repeat {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_repeat")(expr1, expr2)
  }

  /**
    * Sorts the input array in ascending order. The elements of the input array must be orderable.
    * Null elements will be placed at the end of the returned array.
    * @example {{{ array_sort(array) }}}
    */
  object Array_Sort {
    def apply(expr: Expression): FunctionCall = FunctionCall("array_sort")(expr)
  }

  /**
    * Returns an array of the elements in the union of array1 and array2, without duplicates.
    * @example {{{ array_union(array1, array2) }}}
    */
  object Array_Union {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("array_union")(expr1, expr2)
  }

  /**
    * Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.
    * @example {{{ arrays_zip(array1, array2, ...) }}}
    */
  object Arrays_Zip {
    def apply(expr: Expression*): FunctionCall = FunctionCall("arrays_zip")(expr: _*)
  }

  /**
    * Returns the numeric value of the first character of string.
    * @example {{{ ascii(expr) }}}
    */
  object Ascii {
    def apply(expr: Expression): FunctionCall = FunctionCall("ascii")(expr)
  }

  /**
    * Returns the inverse sine (a.k.a. arc sine) the arc sin of expr, as if computed by [[java.lang.Math#asin]].
    * @example {{{ asin(expr) }}}
    */
  object Asin {
    def apply(expr: Expression): FunctionCall = FunctionCall("asin")(expr)
  }

  /**
    * Throws an exception if expr is not true.
    * @example {{{ assert_true(expr) }}}
    */
  object Assert_True {
    def apply(expr: Expression): FunctionCall = FunctionCall("assert_true")(expr)
  }

  /**
    * Returns the inverse tangent (a.k.a. arc tangent) of expr, as if computed by [[java.lang.Math#atan]].
    * @example {{{ atan(expr) }}}
    */
  object Atan {
    def apply(expr: Expression): FunctionCall = FunctionCall("atan")(expr)
  }

  /**
    * Returns the angle in radians between the positive x-axis of a plane and the point given by the
    * coordinates (exprX, exprY), as if computed by [[java.lang.Math#atan2]].
    * @example {{{ atan2(expr1, expr2) }}}
    */
  object Atan2 {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("atan2")(expr1, expr2)
  }

  /**
    * Returns the mean calculated from values of a group.
    * @example {{{ avg(expr) }}}
    */
  object Avg {
    def apply(expr: Expression): FunctionCall = FunctionCall("avg")(expr)
  }

  /**
    * Converts the argument from a binary bin to a base 64 string.
    * @example {{{ base64(expr) }}}
    */
  object Base64 {
    def apply(expr: Expression): FunctionCall = FunctionCall("base64")(expr)
  }

  /**
    * Casts the value expr to the target data type bigint.
    * @example {{{ bigint(expr) }}}
    */
  object Bigint {
    def apply(expr: Expression): FunctionCall = FunctionCall("bigint")(expr)
  }

  /**
    * Returns the string representation of the long value expr represented in binary.
    * @example {{{ bin(expr) }}}
    */
  object Bin {
    def apply(expr: Expression): FunctionCall = FunctionCall("bin")(expr)
  }

  /**
    * Casts the value expr to the target data type binary.
    * @example {{{ binary(expr) }}}
    */
  object Binary {
    def apply(expr: Expression): FunctionCall = FunctionCall("binary")(expr)
  }

  /**
    * Returns the bit length of string data or number of bits of binary data.
    * @example {{{ bit_length(expr) }}}
    */
  object Bit_Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("bit_length")(expr)
  }

  /**
    * Casts the value expr to the target data type boolean.
    * @example {{{ boolean(expr) }}}
    */
  object Boolean {
    def apply(expr: Expression): FunctionCall = FunctionCall("boolean")(expr)
  }

  /**
    * Returns expr rounded to d decimal places using HALF_EVEN rounding mode.
    * @example {{{ bround(expr, d) }}}
    */
  object Bround {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("bround")(expr1, expr2)
  }

  /**
    * Returns the size of an array or a map. The function returns -1 if its input is null and
    *spark.sql.legacy.sizeOfNull is set to true. If spark.sql.legacy.sizeOfNull is set to false,
    * the function returns null for null input. By default, the spark.sql.legacy.sizeOfNull parameter
    * is set to true.
    * @example {{{ cardinality(expr) }}}
    */
  object Cardinality {
    def apply(expr: Expression): FunctionCall = FunctionCall("cardinality")(expr)
  }

  /**
    * Returns the cube root of expr.
    * @example {{{ cbrt(expr) }}}
    */
  object Cbrt {
    def apply(expr: Expression): FunctionCall = FunctionCall("cbrt")(expr)
  }

  /**
    * Returns the smallest integer not smaller than expr.
    * @example {{{ ceil(expr) }}}
    */
  object Ceil {
    def apply(expr: Expression): FunctionCall = FunctionCall("ceil")(expr)
  }

  /**
    * Returns the smallest integer not smaller than expr.
    * @example {{{ ceiling(expr) }}}
    */
  object Ceiling {
    def apply(expr: Expression): FunctionCall = FunctionCall("ceiling")(expr)
  }

  /**
    * Returns the ASCII character having the binary equivalent to expr.
    * If n is larger than 256 the result is equivalent to chr(n % 256).
    * @example {{{ char(expr) }}}
    */
  object Char {
    def apply(expr: Expression): FunctionCall = FunctionCall("char")(expr)
  }

  /**
    * Returns the character length of string data or number of bytes of binary data. The length of
    * string data includes the trailing spaces. The length of binary data includes binary zeros.
    * @example {{{ char_length(expr) }}}
    */
  object Char_Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("char_length")(expr)
  }

  /**
    * Collects and returns a list of non-unique elements.
    * @example {{{ character_length(expr) }}}
    */
  object Character_Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("character_length")(expr)
  }

  /**
    * Returns the ASCII character having the binary equivalent to expr.
    * If n is larger than 256 the result is equivalent to chr(n % 256).
    * @example {{{ chr(expr) }}}
    */
  object Chr {
    def apply(expr: Expression): FunctionCall = FunctionCall("chr")(expr)
  }

  /**
    * Returns the first non-null argument if exists. Otherwise, null.
    * @example {{{ coalesce(expr1, ...) }}}
    */
  object Coalesce {
    def apply(expr: Expression*): FunctionCall = FunctionCall("coalesce")(expr: _*)
  }

  /**
    * Collects and returns a list of non-unique elements.
    * @example {{{ collect_list(expr) }}}
    */
  object Collect_List {
    def apply(expr: Expression): FunctionCall = FunctionCall("collect_list")(expr)
  }

  /**
    * Collects and returns a set of unique elements.
    * @example {{{ collect_set(expr) }}}
    */
  object Collect_Set {
    def apply(expr: Expression): FunctionCall = FunctionCall("collect_set")(expr)
  }

  /**
    * Returns the concatenation of col1, col2, ..., colN.
    * @example {{{ concat(col1, col2, ..., colN) }}}
    */
  object Concat {
    def apply(expr: Expression*): FunctionCall = FunctionCall("concat")(expr: _*)
  }

  /**
    * Returns the concatenation of the strings separated by sep.
    * @example {{{ concat_ws(sep, [str | array(str)]+) }}}
    */
  object Concat_Ws {
    def apply(expr: Expression*): FunctionCall = FunctionCall("concat_ws")(expr: _*)
  }

  /**
    * Convert num from from_base to to_base.
    * @example {{{ conv(num, from_base, to_base) }}}
    */
  object Conv {
    def apply(expr: Expression*): FunctionCall = FunctionCall("conv")(expr: _*)
  }

  /**
    * Returns Pearson coefficient of correlation between a set of number pairs.
    * @example {{{ corr(expr1, expr2) }}}
    */
  object Corr {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("corr")(expr1, expr2)
  }

  /**
    * Returns the cosine of expr, as if computed by [[java.lang.Math#cos]].
    * @example {{{ cos(expr) }}}
    */
  object Cos {
    def apply(expr: Expression): FunctionCall = FunctionCall("cos")(expr)
  }

  /**
    * Returns the hyperbolic cosine of expr, as if computed by [[java.lang.Math#cosh]].
    * @example {{{ cosh(expr) }}}
    */
  object Cosh {
    def apply(expr: Expression): FunctionCall = FunctionCall("cosh")(expr)
  }

  /**
    * Returns the cotangent of expr, as if computed by [[java.lang.Math#cot]].
    * @example {{{ cot(expr) }}}
    */
  object Cot {
    def apply(expr: Expression): FunctionCall = FunctionCall("cot")(expr)
  }

  /**
    * Returns the total number of retrieved rows, including rows containing null.
    * @example {{{ count(expr1, ...) }}}
    */
  object Count {
    def apply(expr: Expression*): FunctionCall = FunctionCall("count")(expr: _*)
  }

  /**
    * Returns a count-min sketch of a column with the given esp, confidence and seed.
    * The result is an array of bytes, which can be deserialized to a CountMinSketch before usage.
    * Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.
    * @example {{{ count_min_sketch(col, eps, confidence, seed) }}}
    */
  object Count_Min_Sketch {
    def apply(expr: Expression*): FunctionCall = FunctionCall("count_min_sketch")(expr: _*)
  }

  /**
    * Returns the population covariance of a set of number pairs.
    * @example {{{ covar_pop(expr1, expr2) }}}
    */
  object Covar_Pop {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("covar_pop")(expr1, expr2)
  }

  /**
    * Returns the sample covariance of a set of number pairs.
    * @example {{{ covar_samp(expr1, expr2) }}}
    */
  object Covar_Samp {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("covar_samp")(expr1, expr2)
  }

  /**
    * Returns a cyclic redundancy check value of the expr as a bigint.
    * @example {{{ crc32(expr) }}}
    */
  object Crc32 {
    def apply(expr: Expression): FunctionCall = FunctionCall("crc32")(expr)
  }

  /**
    * [[https://spark.apache.org/docs/2.4.0/api/sql/index.html#cube]].
    * @example {{{ cube(expr1, ...) }}}
    */
  object Cube {
    def apply(expr: Expression*): FunctionCall = FunctionCall("cube")(expr: _*)
  }

  /**
    * Computes the position of a value relative to all values in the partition.
    * @example {{{ cume_dist() }}}
    */
  object Cume_Dist {
    def apply(): FunctionCall = FunctionCall("cume_dist")()
  }

  /**
    * Returns the current database.
    * @example {{{ current_database() }}}
    */
  object Current_Database {
    def apply(): FunctionCall = FunctionCall("current_database")()
  }

  /**
    * Returns the current date at the start of query evaluation.
    * @example {{{ current_date() }}}
    */
  object Current_Date {
    def apply(): FunctionCall = FunctionCall("current_date")()
  }

  /**
    * Returns the current timestamp at the start of query evaluation.
    * @example {{{ current_timestamp() }}}
    */
  object Current_Timestamp {
    def apply(): FunctionCall = FunctionCall("current_timestamp")()
  }

  /**
    * Casts the value expr to the target data type date.
    * @example {{{ date(expr) }}}
    */
  object Date {
    def apply(expr: Expression): FunctionCall = FunctionCall("date")(expr)
  }

  /**
    * Returns the date that is num_days after start_date.
    * @example {{{ date_add(start_date, num_days) }}}
    */
  object Date_Add {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("date_add")(expr1, expr2)
  }

  /**
    * Converts timestamp to a value of string in the format specified by the date format fmt.
    * @example {{{ date_format(timestamp, fmt) }}}
    */
  object Date_Format {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("date_format")(expr1, expr2)
  }

  /**
    * Returns the date that is num_days before start_date.
    * @example {{{ date_sub(start_date, num_days) }}}
    */
  object Date_Sub {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("date_sub")(expr1, expr2)
  }

  /**
    * Returns timestamp ts truncated to the unit specified by the format model fmt.
    * fmt should be one of ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD",
    * "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"].
    * @example {{{ date_trunc(expr1, expr2) }}}
    */
  object Date_Trunc {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("date_trunc")(expr1, expr2)
  }

  /**
    * Returns the number of days from startDate to endDate.
    * @example {{{ datediff(endDate, startDate) }}}
    */
  object Datediff {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("datediff")(expr1, expr2)
  }

  /**
    * Returns the day of month of the date/timestamp.
    * @example {{{ day(date) }}}
    */
  object Day {
    def apply(expr: Expression): FunctionCall = FunctionCall("day")(expr)
  }

  /**
    * Returns the day of month of the date/timestamp.
    * @example {{{ dayofmonth(date) }}}
    */
  object Dayofmonth {
    def apply(expr: Expression): FunctionCall = FunctionCall("dayofmonth")(expr)
  }

  /**
    * Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
    * @example {{{ dayofweek(date) }}}
    */
  object Dayofweek {
    def apply(expr: Expression): FunctionCall = FunctionCall("dayofweek")(expr)
  }

  /**
    * Casts the value expr to the target data type decimal.
    * @example {{{ decimal(expr) }}}
    */
  object Decimal {
    def apply(expr: Expression): FunctionCall = FunctionCall("decimal")(expr)
  }

  /**
    * Decodes the first argument using the second argument character set.
    * @example {{{ decode(bin, charset) }}}
    */
  object Decode {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("decode")(expr1, expr2)
  }

  /**
    * Converts radians to degrees.
    * @example {{{ degrees(expr) }}}
    */
  object Degrees {
    def apply(expr: Expression): FunctionCall = FunctionCall("degrees")(expr)
  }

  /**
    * Computes the rank of a value in a group of values. The result is one plus the previously assigned rank value.
    * Unlike the function rank, dense_rank will not produce gaps in the ranking sequence.
    * @example {{{ dense_rank() }}}
    */
  object Dense_Rank {
    def apply(): FunctionCall = FunctionCall("dense_rank")()
  }

  /**
    * .
    * @example {{{ distinct(expr) }}}
    */
  object Distinct {
    def apply(expr: Expression): FunctionCall = FunctionCall("distinct")(expr)
  }

  /**
    * Casts the value expr to the target data type double.
    * @example {{{ double(expr) }}}
    */
  object Double {
    def apply(expr: Expression): FunctionCall = FunctionCall("double")(expr)
  }

  /**
    * Returns Euler's number, e.
    * @example {{{ e() }}}
    */
  object E {
    def apply(): FunctionCall = FunctionCall("e")()
  }

  /**
    * Returns the n-th input, e.g., returns input2 when n is 2.
    * @example {{{ elt(n, input1, input2, ...) }}}
    */
  object Elt {
    def apply(expr: Expression*): FunctionCall = FunctionCall("elt")(expr: _*)
  }

  /**
    * Encodes the first argument using the second argument character set.
    * @example {{{ encode(str, charset) }}}
    */
  object Encode {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("encode")(expr1, expr2)
  }

  /**
    * Returns e to the power of expr.
    * @example {{{ exp(expr) }}}
    */
  object Exp {
    def apply(expr: Expression): FunctionCall = FunctionCall("exp")(expr)
  }

  /**
    * Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.
    * @example {{{ explode(expr) }}}
    */
  object Explode {
    def apply(expr: Expression): FunctionCall = FunctionCall("explode")(expr)
  }

  /**
    * Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.
    * @example {{{ explode_outer(expr) }}}
    */
  object Explode_Outer {
    def apply(expr: Expression): FunctionCall = FunctionCall("explode_outer")(expr)
  }

  /**
    * Returns exp(expr) - 1.
    * @example {{{ expm1(expr) }}}
    */
  object Expm1 {
    def apply(expr: Expression): FunctionCall = FunctionCall("expm1")(expr)
  }

  /**
    * Returns the factorial of expr. expr is [0..20]. Otherwise, null.
    * @example {{{ factorial(expr) }}}
    */
  object Factorial {
    def apply(expr: Expression): FunctionCall = FunctionCall("factorial")(expr)
  }

  /**
    * Returns the index (1-based) of the given string (str) in the comma-delimited list (str_array).
    * Returns 0, if the string was not found or if the given string (str) contains a comma.
    * @example {{{ find_in_set(expr1, expr2) }}}
    */
  object Find_In_Set {
    def apply(expr: Expression*): FunctionCall = FunctionCall("find_in_set")(expr: _*)
  }

  /**
    * Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values.
    * @example {{{ first(expr[, isIgnoreNull]) }}}
    */
  object First {
    def apply(expr: Expression*): FunctionCall = FunctionCall("first")(expr: _*)
  }

  /**
    * Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values.
    * @example {{{ first_value(expr[, isIgnoreNull]) }}}
    */
  object First_Value {
    def apply(expr: Expression*): FunctionCall = FunctionCall("first_value")(expr: _*)
  }

  /**
    * Transforms an array of arrays into a single array.
    * @example {{{ flatten(arrayOfArrays) }}}
    */
  object Flatten {
    def apply(expr: Expression): FunctionCall = FunctionCall("flatten")(expr)
  }

  /**
    * Casts the value expr to the target data type float.
    * @example {{{ float(expr) }}}
    */
  object Float {
    def apply(expr: Expression): FunctionCall = FunctionCall("float")(expr)
  }

  /**
    * Returns the largest integer not greater than expr.
    * @example {{{ floor(expr) }}}
    */
  object Floor {
    def apply(expr: Expression): FunctionCall = FunctionCall("floor")(expr)
  }

  /**
    * Formats the number expr1 like '#,###,###.##', rounded to expr2 decimal places.
    * If expr2 is 0, the result has no decimal point or fractional part. expr2 also accept a user specified format.
    * This is supposed to function like MySQL's FORMAT.
    * @example {{{ format_number(expr1, expr2) }}}
    */
  object Format_Number {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("format_number")(expr1, expr2)
  }

  /**
    * Returns a formatted string from printf-style format strings.
    * @example {{{ format_string(strfmt, obj, ...) }}}
    */
  object Format_String {
    def apply(expr: Expression*): FunctionCall = FunctionCall("format_string")(expr: _*)
  }

  /**
    * Returns a struct value with the given jsonStr and schema.
    * @example {{{ from_json(jsonStr, schema[, options]) }}}
    */
  object From_Json {
    def apply(expr: Expression*): FunctionCall = FunctionCall("from_json")(expr: _*)
  }

  /**
    * Returns unix_time in the specified format.
    * @example {{{ from_unixtime(unix_time, format) }}}
    */
  object From_Unixtime {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("from_unixtime")(expr1, expr2)
  }

  /**
    * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC,
    * and renders that time as a timestamp in the given time zone.
    * For example, 'GMT+1' would yield '2017-07-14 03:40:00.0'.
    * @example {{{ from_utc_timestamp(timestamp, timezone) }}}
    */
  object From_Utc_Timestamp {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("from_utc_timestamp")(expr1, expr2)
  }

  /**
    * Extracts a json object from path.
    * @example {{{ get_json_object(json_txt, path) }}}
    */
  object Get_Json_Object {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("get_json_object")(expr1, expr2)
  }

  /**
    * Returns the greatest value of all parameters, skipping null values.
    * @example {{{ greatest(expr1, ...) }}}
    */
  object Greatest {
    def apply(expr: Expression*): FunctionCall = FunctionCall("greatest")(expr: _*)
  }

  /**
    * Returns a hash value of the arguments.
    * @example {{{ hash(expr1, ...) }}}
    */
  object Hash {
    def apply(expr: Expression*): FunctionCall = FunctionCall("hash")(expr: _*)
  }

  /**
    * Converts expr to hexadecimal.
    * @example {{{ hex(expr) }}}
    */
  object Hex {
    def apply(expr: Expression): FunctionCall = FunctionCall("hex")(expr)
  }

  /**
    * Returns the hour component of the string/timestamp.
    * @example {{{ hour(timestamp) }}}
    */
  object Hour {
    def apply(expr: Expression): FunctionCall = FunctionCall("hour")(expr)
  }

  /**
    * Returns sqrt(expr1^2 + expr2^2).
    * @example {{{ hypot(expr1, expr2) }}}
    */
  object Hypot {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("hypot")(expr1, expr2)
  }

  /**
    * Returns true if expr equals to any valN.
    * @example {{{ in(expr1, ...) }}}
    */
  object In {
    def apply(expr: Expression*): FunctionCall = FunctionCall("in")(expr: _*)
  }

  /**
    * Returns str with the first letter of each word in uppercase. All other letters are in lowercase.
    * Words are delimited by white space.
    * @example {{{ initcap(str) }}}
    */
  object Initcap {
    def apply(expr: Expression): FunctionCall = FunctionCall("initcap")(expr)
  }

  /**
    * Explodes an array of structs into a table.
    * @example {{{ inline(expr) }}}
    */
  object Inline {
    def apply(expr: Expression): FunctionCall = FunctionCall("inline")(expr)
  }

  /**
    * Explodes an array of structs into a table.
    * @example {{{ inline_outer(expr) }}}
    */
  object Inline_Outer {
    def apply(expr: Expression): FunctionCall = FunctionCall("inline_outer")(expr)
  }

  /**
    * Returns the length of the block being read, or -1 if not available.
    * @example {{{ input_file_block_length() }}}
    */
  object Input_File_Block_Length {
    def apply(): FunctionCall = FunctionCall("input_file_block_length")()
  }

  /**
    * Returns the start offset of the block being read, or -1 if not available.
    * @example {{{ input_file_block_start() }}}
    */
  object Input_File_Block_Start {
    def apply(): FunctionCall = FunctionCall("input_file_block_start")()
  }

  /**
    * Returns the name of the file being read, or empty string if not available.
    * @example {{{ input_file_name() }}}
    */
  object Input_File_Name {
    def apply(): FunctionCall = FunctionCall("input_file_name")()
  }

  /**
    * Returns the (1-based) index of the first occurrence of substr in str.
    * @example {{{ instr(str, substr) }}}
    */
  object Instr {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("instr")(expr1, expr2)
  }

  /**
    * Casts the value expr to the target data type int.
    * @example {{{ int(expr) }}}
    */
  object Int {
    def apply(expr: Expression): FunctionCall = FunctionCall("int")(expr)
  }

  /**
    * Returns true if expr is NaN, or false otherwise.
    * @example {{{ isnan(expr) }}}
    */
  object Isnan {
    def apply(expr: Expression): FunctionCall = FunctionCall("isnan")(expr)
  }

  /**
    * Returns true if expr is not null, or false otherwise.
    * @example {{{ isnotnull(expr) }}}
    */
  object Isnotnull {
    def apply(expr: Expression): FunctionCall = FunctionCall("isnotnull")(expr)
  }

  /**
    * Returns true if expr is null, or false otherwise.
    * @example {{{ isnull(expr) }}}
    */
  object Isnull {
    def apply(expr: Expression): FunctionCall = FunctionCall("isnull")(expr)
  }

  /**
    * Calls a method with reflection.
    * @example {{{ java_method(class, method[, arg1[, arg2 ..]]) }}}
    */
  object Java_Method {
    def apply(expr: Expression*): FunctionCall = FunctionCall("java_method")(expr: _*)
  }

  /**
    * Returns a tuple like the function get_json_object, but it takes multiple names.
    * All the input parameters and output column types are string.
    * @example {{{ json_tuple(jsonStr, p1, p2, ..., pn) }}}
    */
  object Json_Tuple {
    def apply(expr: Expression*): FunctionCall = FunctionCall("json_tuple")(expr: _*)
  }

  /**
    * Returns the kurtosis value calculated from values of a group.
    * @example {{{ kurtosis(expr) }}}
    */
  object Kurtosis {
    def apply(expr: Expression): FunctionCall = FunctionCall("kurtosis")(expr)
  }

  /**
    * Returns the value of input at the offsetth row before the current row in the window. The default value of
    * offset is 1 and the default value of default is null. If the value of  input at the offsetth row is null,
    * null is returned. If there is no such offset row (e.g., when the offset is 1, the first row of the window
    * does not have any previous row), default is returned.
    * @example {{{ lag(input[, offset[, default]]) }}}
    */
  object Lag {
    def apply(expr: Expression*): FunctionCall = FunctionCall("lag")(expr: _*)
  }

  /**
    * Returns the last value of expr for a group of rows. If isIgnoreNull is true,
    * returns only non-null values.
    * @example {{{ last(expr[, isIgnoreNull]) }}}
    */
  object Last {
    def apply(expr: Expression*): FunctionCall = FunctionCall("last")(expr: _*)
  }

  /**
    * Returns the last day of the month which the date belongs to.
    * @example {{{ last_day(date) }}}
    */
  object Last_Day {
    def apply(expr: Expression): FunctionCall = FunctionCall("last_day")(expr)
  }

  /**
    * Returns the last value of expr for a group of rows. If isIgnoreNull is true,
    * returns only non-null values.
    * @example {{{ last_value(expr[, isIgnoreNull]) }}}
    */
  object Last_Value {
    def apply(expr: Expression*): FunctionCall = FunctionCall("last_value")(expr: _*)
  }

  /**
    * Returns str with all characters changed to lowercase.
    * @example {{{ lcase(str) }}}
    */
  object Lcase {
    def apply(expr: Expression): FunctionCall = FunctionCall("lcase")(expr)
  }

  /**
    * Returns the value of input at the offsetth row after the current row in the window. The default value of
    * offset is 1 and the default value of default is null. If the value of  input at the offsetth row is null,
    * null is returned. If there is no such an offset row (e.g., when the offset is 1, the last row of the window
    * does not have any subsequent row), default is returned.
    * @example {{{ lead(input[, offset[, default]])  }}}
    */
  object Lead {
    def apply(expr: Expression*): FunctionCall = FunctionCall("lead")(expr: _*)
  }

  /**
    * Returns the least value of all parameters, skipping null values.
    * @example {{{ least(expr1, ...) }}}
    */
  object Least {
    def apply(expr: Expression*): FunctionCall = FunctionCall("least")(expr: _*)
  }

  /**
    * Returns the leftmost len(len can be string type) characters from the string str,
    * if len is less or equal than 0 the result is an empty string.
    * @example {{{ left(str, len) }}}
    */
  object Left {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("left")(expr1, expr2)
  }

  /**
    * Returns the character length of string data or number of bytes of binary data. The length of string data
    * includes the trailing spaces. The length of binary data includes binary zeros.
    * @example {{{ length(expr) }}}
    */
  object Length {
    def apply(expr: Expression): FunctionCall = FunctionCall("length")(expr)
  }

  /**
    * Returns the Levenshtein distance between the two given strings.
    * @example {{{ levenshtein(str1, str2) }}}
    */
  object Levenshtein {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("levenshtein")(expr1, expr2)
  }

  /**
    * Returns the natural logarithm (base e) of `expr`.
    * @example {{{ ln(expr) }}}
    */
  object Ln {
    def apply(expr: Expression): FunctionCall = FunctionCall("ln")(expr)
  }

  /**
    * Returns the position of the first occurrence of substr in str after position pos.
    * The given pos and return value are 1-based.
    * @example {{{ locate(substr, str[, pos]) }}}
    */
  object Locate {
    def apply(expr: Expression*): FunctionCall = FunctionCall("locate")(expr: _*)
  }

  /**
    * Returns the logarithm of `expr` with `base`.
    * @example {{{ log(base, expr) }}}
    */
  object Log {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("log")(expr1, expr2)
  }

  /**
    * Returns the logarithm of `expr` with base 10.
    * @example {{{ log10(expr) }}}
    */
  object Log10 {
    def apply(expr: Expression): FunctionCall = FunctionCall("log10")(expr)
  }

  /**
    * Returns log(1 + `expr`).
    * @example {{{ log1p(expr) }}}
    */
  object Log1p {
    def apply(expr: Expression): FunctionCall = FunctionCall("log1p")(expr)
  }

  /**
    * Returns the logarithm of `expr` with base 2.
    * @example {{{ log2(expr) }}}
    */
  object Log2 {
    def apply(expr: Expression): FunctionCall = FunctionCall("log2")(expr)
  }

  /**
    * Returns `str` with all characters changed to lowercase.
    * @example {{{ lower(str) }}}
    */
  object Lower {
    def apply(expr: Expression): FunctionCall = FunctionCall("lower")(expr)
  }

  /**
    * Returns `str`, left-padded with pad to a length of len. If `str` is longer than len,
    * the return value is shortened to len characters.
    * @example {{{ lpad(str, len, pad) }}}
    */
  object Lpad {
    def apply(expr1: Expression, expr2: Expression, expr3: Expression): FunctionCall = FunctionCall("lpad")(expr1, expr2, expr3)
  }

  /**
    * Removes the leading space characters from `str`.
    * @example {{{ ltrim([trimStr, ]str) }}}
    */
  object Ltrim {
    def apply(expr: Expression*): FunctionCall = FunctionCall("ltrim")(expr: _*)
  }

  /**
    * Creates a map with the given key/value pairs.
    * @example {{{ map(key0, value0, key1, value1, ...) }}}
    */
  object Map {
    def apply(expr: Expression*): FunctionCall = FunctionCall("map")(expr: _*)
  }

  /**
    * Returns the union of all the given maps.
    * @example {{{ map_concat(map, ...) }}}
    */
  object Map_Concat {
    def apply(expr: Expression*): FunctionCall = FunctionCall("map_concat")(expr: _*)
  }

  /**
    * Creates a map with a pair of the given key/value arrays. All elements in keys should not be null.
    * @example {{{ map_from_arrays(keys, values) }}}
    */
  object Map_From_Arrays {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("map_from_arrays")(expr1, expr2)
  }

  /**
    * Returns a map created from the given array of entries.
    * @example {{{ map_from_entries(arrayOfEntries) }}}
    */
  object Map_From_Entries {
    def apply(expr: Expression*): FunctionCall = FunctionCall("map_from_entries")(expr: _*)
  }

  /**
    * Returns an unordered array containing the keys of the map.
    * @example {{{ map_keys(map) }}}
    */
  object Map_Keys {
    def apply(expr: Expression): FunctionCall = FunctionCall("map_keys")(expr)
  }

  /**
    * Returns an unordered array containing the values of the map.
    * @example {{{ map_values(map) }}}
    */
  object Map_Values {
    def apply(expr: Expression): FunctionCall = FunctionCall("map_values")(expr)
  }

  /**
    * Returns the maximum value of `expr`.
    * @example {{{ max(expr) }}}
    */
  object Max {
    def apply(expr: Expression): FunctionCall = FunctionCall("max")(expr)
  }

  /**
    * Returns an MD5 128-bit checksum as a hex string of `expr`.
    * @example {{{ md5(expr) }}}
    */
  object Md5 {
    def apply(expr: Expression): FunctionCall = FunctionCall("md5")(expr)
  }

  /**
    * Returns the mean calculated from values of a group.
    * @example {{{ mean(expr) }}}
    */
  object Mean {
    def apply(expr: Expression): FunctionCall = FunctionCall("mean")(expr)
  }

  /**
    * Returns the minimum value of `expr`.
    * @example {{{ min(expr) }}}
    */
  object Min {
    def apply(expr: Expression): FunctionCall = FunctionCall("min")(expr)
  }

  /**
    * Returns the minute component of the string/timestamp.
    * @example {{{ minute(timestamp) }}}
    */
  object Minute {
    def apply(expr: Expression): FunctionCall = FunctionCall("minute")(expr)
  }

  /**
    * monotonically_increasing_id() - Returns monotonically increasing 64-bit integers.
    * The generated ID is guaranteed to be monotonically increasing and unique, but not consecutive.
    * The current implementation puts the partition ID in the upper 31 bits, and the lower 33 bits
    * represent the record number within each partition. The assumption is that the data frame has
    * less than 1 billion partitions, and each partition has less than 8 billion records. The function
    * is non-deterministic because its result depends on partition IDs.
    * @example {{{ monotonically_increasing_id() }}}
    */
  object Monotonically_Increasing_Id {
    def apply(): FunctionCall = FunctionCall("monotonically_increasing_id")()
  }

  /**
    * Returns the month component of the date/timestamp.
    * @example {{{ month(date) }}}
    */
  object Month {
    def apply(expr: Expression): FunctionCall = FunctionCall("month")(expr)
  }

  /**
    * If timestamp1 is later than timestamp2, then the result is positive. If timestamp1 and timestamp2 are
    * on the same day of month, or both are the last day of month, time of day will be ignored. Otherwise,
    * the difference is calculated based on 31 days per month, and rounded to 8 digits unless roundOff=false.
    * @example {{{ months_between(timestamp1, timestamp2[, roundOff])  }}}
    */
  object Months_Between {
    def apply(expr: Expression*): FunctionCall = FunctionCall("months_between")(expr: _*)
  }

  /**
    * Creates a struct with the given field names and values.
    * @example {{{ named_struct(name1, val1, name2, val2, ...) }}}
    */
  object Named_Struct {
    def apply(expr: Expression*): FunctionCall = FunctionCall("named_struct")(expr: _*)
  }

  /**
    * Returns `expr1` if it's not NaN, or `expr2` otherwise.
    * @example {{{ nanvl(expr1, expr2) }}}
    */
  object Nanvl {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("nanvl")(expr1, expr2)
  }

  /**
    * Returns the negated value of `expr`.
    * @example {{{ negative(expr) }}}
    */
  object Negative {
    def apply(expr: Expression): FunctionCall = FunctionCall("negative")(expr)
  }

  /**
    * Returns the first date which is later than start_date and named as indicated.
    * @example {{{ next_day(start_date, day_of_week) }}}
    */
  object Next_Day {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("next_day")(expr1, expr2)
  }

  /**
    * Returns the current timestamp at the start of query evaluation.
    * @example {{{ now() }}}
    */
  object Now {
    def apply(): FunctionCall = FunctionCall("now")()
  }

  /**
    * Divides the rows for each window partition into n buckets ranging from 1 to at most `n`.
    * @example {{{ ntile(n) }}}
    */
  object Ntile {
    def apply(expr: Expression): FunctionCall = FunctionCall("ntile")(expr)
  }

  /**
    * Returns null if `expr1` equals to `expr2`, or `expr1` otherwise.
    * @example {{{ nullif(expr1, expr2) }}}
    */
  object Nullif {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("nullif")(expr1, expr2)
  }

  /**
    * Extracts a part from a URL.
    * @example {{{ parse_url(url, partToExtract[, key]) }}}
    */
  object Parse_Url {
    def apply(expr: Expression*): FunctionCall = FunctionCall("parse_url")(expr: _*)
  }

  /**
    * Calculates the rank of a value in a group of values.
    * @example {{{ rank() over(window_spec)) }}}
    */
  object Rank {
    def apply(): FunctionCall = FunctionCall("rank")()
  }

  /**
    * Returns a reversed string or an array with reverse order of elements.
    * @example {{{ reverse(array) }}}
    */
  object Reverse {
    def apply(expr: Expression): FunctionCall = FunctionCall("reverse")(expr)
  }

  /**
    * Assigns a unique number to each row to which it is applied.
    * @example {{{ row_number() over(window_spec)) }}}
    */
  object Row_Number {
    def apply(): FunctionCall = FunctionCall("row_number")()
  }

  /**
    * Returns str, right-padded with pad to a length of len. If str is longer than len,
    * the return value is shortened to len characters.
    * @example {{{ rpad(str, len, pad) }}}
    */
  object Rpad {
    def apply(expr1: Expression, expr2: Expression, expr3: Expression): FunctionCall = FunctionCall("rpad")(expr1, expr2, expr3)
  }

  /**
    * Removes the trailing space characters from `str`.
    * @example {{{ rtrim(str) }}}
    */
  object Rtrim {
    def apply(expr: Expression): FunctionCall = FunctionCall("rtrim")(expr)
  }

  /**
    * Splits `str` around occurrences that match `regex`.
    * @example {{{ split(str, regex) }}}
    */
  object Split {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("split")(expr1, expr2)
  }

  /**
    * Returns the sample standard deviation calculated from values of a group.
    * @example {{{ stddev(expr) }}}
    */
  object Stddev {
    def apply(expr: Expression): FunctionCall = FunctionCall("stddev")(expr)
  }

  /**
    * Returns the substring of `str` that starts at pos and is of length `len`, or the slice of byte array that
    * starts at `pos` and is of length `len`.
    * @example {{{ substr(str, pos[, len]) }}}
    */
  object Substr {
    def apply(expr: Expression*): FunctionCall = FunctionCall("substr")(expr: _*)
  }

  /**
    * Returns the substring of str that starts at `pos` and is of length `len`, or the slice of byte array that
    * starts at pos and is of length `len`.
    * @example {{{ substring(str, pos[, len]) }}}
    */
  object Substring {
    def apply(expr: Expression*): FunctionCall = FunctionCall("substring")(expr: _*)
  }

  /**
    * Returns the substring from str before count occurrences of the delimiter delim. If count is positive,
    * everything to the left of the final delimiter (counting from the left) is returned. If count is negative,
    * everything to the right of the final delimiter (counting from the right) is returned. The function
    * substring_index performs a case-sensitive match when searching for  delim.
    * @example {{{ substring_index(str, delim, count) }}}
    */
  object Substring_Index {
    def apply(expr1: Expression, expr2: Expression, expr3: Expression): FunctionCall = FunctionCall("substring_index")(expr1, expr2, expr3)
  }

  /**
    * Returns the sum calculated from values of a group.
    * @example {{{ sum(expr) }}}
    */
  object Sum {
    def apply(expr: Expression): FunctionCall = FunctionCall("sum")(expr)
  }

  /**
    * Parses the date_str expression with the fmt expression to a date. Returns null with invalid input.
    * By default, it follows casting rules to a date if the fmt is omitted.
    * @example {{{ to_date(date_str[, fmt]) }}}
    */
  object To_Date {
    def apply(expr: Expression*): FunctionCall = FunctionCall("to_date")(expr: _*)
  }

  /**
    * Translates the input string by replacing the characters present in the from string with the
    * corresponding characters in the to string..
    * @example {{{ translate(input, from, to) }}}
    */
  object Translate {
    def apply(expr1: Expression, expr2: Expression, expr3: Expression): FunctionCall = FunctionCall("translate")(expr1, expr2, expr3)
  }

  /**
    * Removes the leading and trailing space characters from `str`.
    * @example {{{ trim(str) }}}
    */
  object Trim {
    def apply(expr: Expression): FunctionCall = FunctionCall("trim")(expr)
  }

  /**
    * Returns `str` with all characters changed to uppercase.
    * @example {{{ ucase(str) }}}
    */
  object Ucase {
    def apply(expr: Expression): FunctionCall = FunctionCall("ucase")(expr)
  }

  /**
    * Converts the argument from a base 64 string `str` to a binary.
    * @example {{{ unbase64(str) }}}
    */
  object Unbase64 {
    def apply(expr: Expression): FunctionCall = FunctionCall("unbase64")(expr)
  }

  /**
    * Converts hexadecimal expr to binary.
    * @example {{{ unhex(expr) }}}
    */
  object Unhex {
    def apply(expr: Expression): FunctionCall = FunctionCall("unhex")(expr)
  }

  /**
    * Returns str with all characters changed to uppercase.
    * @example {{{ upper(str) }}}
    */
  object Upper {
    def apply(expr: Expression): FunctionCall = FunctionCall("upper")(expr)
  }

  /**
    * Returns an universally unique identifier (UUID) string. The value is returned as a canonical UUID 36-character string.
    * @example {{{ uuid() }}}
    */
  object Uuid {
    def apply(): FunctionCall = FunctionCall("uuid")()
  }

  /**
    * Returns the population variance calculated from values of a group.
    * @example {{{ var_pop(expr) }}}
    */
  object Var_Pop {
    def apply(expr: Expression): FunctionCall = FunctionCall("var_pop")(expr)
  }

  /**
    * Returns the sample variance calculated from values of a group.
    * @example {{{ var_samp(expr) }}}
    */
  object Var_Samp {
    def apply(expr: Expression): FunctionCall = FunctionCall("var_samp")(expr)
  }

  /**
    * .
    * @example {{{ variance(expr) }}}
    */
  object Variance {
    def apply(expr: Expression): FunctionCall = FunctionCall("variance")(expr)
  }

  /**
    * weekday(date) - Returns the day of the week for date/timestamp (0 = Monday, 1 = Tuesday, ..., 6 = Sunday).
    * @example {{{ weekday(date) }}}
    */
  object Weekday {
    def apply(expr: Expression): FunctionCall = FunctionCall("weekday")(expr)
  }

  /**
    * Returns the week of the year of the given date.
    * A week is considered to start on a Monday and week 1 is the first week with >3 days.
    * @example {{{ weekofyear(date) }}}
    */
  object Weekofyear {
    def apply(expr: Expression): FunctionCall = FunctionCall("weekofyear")(expr)
  }

  /**
    * Returns a string array of values within the nodes of xml that match the XPath expression.
    * @example {{{ xpath(xml, xpath) }}}
    */
  object Xpath {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath")(expr1, expr2)
  }

  /**
    * Returns true if the XPath expression evaluates to true, or if a matching node is found.
    * @example {{{ xpath_boolean(xml, xpath) }}}
    */
  object Xpath_Boolean {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_boolean")(expr1, expr2)
  }

  /**
    * Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    * @example {{{ xpath_double(xml, xpath) }}}
    */
  object Xpath_Double {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_double")(expr1, expr2)
  }

  /**
    * Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    * @example {{{ xpath_float(xml, xpath) }}}
    */
  object Xpath_Float {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_float")(expr1, expr2)
  }

  /**
    * Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    * @example {{{ xpath_int(xml, xpath) }}}
    */
  object Xpath_Int {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_int")(expr1, expr2)
  }

  /**
    * Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    * @example {{{ xpath_long(xml, xpath) }}}
    */
  object Xpath_Long {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_long")(expr1, expr2)
  }

  /**
    * Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.
    * @example {{{ xpath_number(xml, xpath) }}}
    */
  object Xpath_Number {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_number")(expr1, expr2)
  }

  /**
    * Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.
    * @example {{{ xpath_short(xml, xpath) }}}
    */
  object Xpath_Short {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_short")(expr1, expr2)
  }

  /**
    * Returns the text contents of the first xml node that matches the XPath expression.
    * @example {{{ xpath_string(xml, xpath) }}}
    */
  object Xpath_String {
    def apply(expr1: Expression, expr2: Expression): FunctionCall = FunctionCall("xpath_string")(expr1, expr2)
  }

  /**
    * Returns the year component of the date/timestamp.
    * @example {{{ year(date) }}}
    */
  object Year {
    def apply(expr: Expression): FunctionCall = FunctionCall("year")(expr)
  }

  /**
    * Merges the two given arrays, element-wise, into a single array using function. If one array is shorter,
    * nulls are appended at the end to match the length of the longer array, before applying function.
    * @example {{{ zip_with(left, right, func) }}}
    */
  object Zip_With {
    def apply(expr1: Expression, expr2: Expression, expr3: Expression): FunctionCall = FunctionCall("zip_with")(expr1, expr2, expr3)
  }

}

