package com.qwery.models.expressions

import com.qwery.models.ColumnTypes.ColumnType

/**
  * Represents a SQL function
  * @author lawrence.daniels@gmail.com
  * @see [[https://spark.apache.org/docs/2.4.1/api/sql/index.html]]
  */
sealed trait SQLFunction extends NamedExpression {
  val name: String = getClass.getSimpleName.toLowerCase().replaceAllLiterally("$", "")
  override val toString: String = s"$name()"
}

/**
  * Represents a SQL function with a single parameter
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction1 extends SQLFunction {
  override val toString = s"$name($field)"

  def field: Expression
}

/**
  * Represents a SQL function with two parameters
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction2 extends SQLFunction {
  override val toString = s"$name($field1,$field2)"

  def field1: Expression

  def field2: Expression
}

/**
  * Represents a SQL function with three parameters
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunction3 extends SQLFunction {
  override val toString = s"$name($field1,$field2,$field3)"

  def field1: Expression

  def field2: Expression

  def field3: Expression
}

/**
  * Represents a SQL function with any number of parameters
  * @author lawrence.daniels@gmail.com
  */
sealed trait SQLFunctionN extends SQLFunction {
  override val toString = s"$name(${args mkString ","})"

  def args: List[Expression]
}

/**
  * Represents an aggregation function
  * @author lawrence.daniels@gmail.com
  */
sealed trait Aggregation extends NamedExpression {
  override val isAggregate = true
}

/**
  * SQLFunction Companion
  * @author lawrence.daniels@gmail.com
  */
object SQLFunction {

  /////////////////////////////////////////////////////////////////////
  //    SQL Function Library
  /////////////////////////////////////////////////////////////////////

  /**
    * Returns the absolute value of the numeric value.
    * @example abs(expr)
    */
  case class Abs(field: Expression) extends SQLFunction1

  /**
    * Returns the inverse cosine (a.k.a. arc cosine) of expr, as if computed by [[java.lang.Math.acos]].
    * @example acos(expr)
    */
  case class Acos(field: Expression) extends SQLFunction1

  /**
    * Returns the date that is num_months after start_date.
    * @example add_months(start_date, num_months)
    */
  case class Add_Months(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Applies a binary operator to an initial state and all elements in the array, and reduces this to a single state.
    * The final state is converted into the final result by applying a finish function.
    * @example aggregate(expr, start, merge, finish)
    */
  case class Aggregate(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns the estimated cardinality by HyperLogLog++. relativeSD defines the maximum estimation error allowed.
    * @example approx_count_distinct(expr[, relativeSD])
    */
  case class Approx_Count_Distinct(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns the approximate percentile value of numeric column col at the given percentage.
    * The value of percentage must be between 0.0 and 1.0. The  accuracy parameter (default: 10000)
    * is a positive numeric literal which controls approximation accuracy at the cost of memory.
    * Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the approximation.
    * When percentage is an array, each value of the percentage array must be between 0.0 and 1.0.
    * In this case, returns the approximate percentile array of column col at the given percentage array.
    * @example approx_percentile(col, percentage [, accuracy])
    */
  case class Approx_Percentile(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns an array with the given elements.
    * @example array(expr, ...)
    */
  case class Array(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns true if the array contains the value.
    * @example array_contains(array, value)
    */
  case class Array_Contains(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Removes duplicate values from the array.
    * @example array_distinct(array)
    */
  case class Array_Distinct(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns an array of the elements in array1 but not in array2, without duplicates.
    * @example array_except(array1, array2)
    */
  case class Array_Except(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns an array of the elements in the intersection of array1 and array2, without duplicates.
    * @example array_intersect(array1, array2)
    */
  case class Array_Intersect(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Concatenates the elements of the given array using the delimiter and an optional string to replace nulls. If no value is set for nullReplacement, any null value is filtered.
    * @example array_join(array, delimiter[, nullReplacement])
    */
  case class Array_Join(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns the maximum value in the array. NULL elements are skipped.
    * @example array_max(array)
    */
  case class Array_Max(field: Expression) extends SQLFunction1

  /**
    * Returns the minimum value in the array. NULL elements are skipped.
    * @example array_min(array)
    * @example array_min(array(1, 20, null, 3)) ~> 1
    */
  case class Array_Min(field: Expression) extends SQLFunction1

  /**
    * Returns true if a1 contains at least a non-null element present also in a2. If the arrays have no common element and they are both non-empty and either of them contains a null element null is returned, false otherwise.
    * @example arrays_overlap(a1, a2)
    */
  case class Array_Overlap(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the (1-based) index of the first element of the array as long.
    * @example array_position(array, element)
    * @example array_position(array(3, 2, 1), 1) ~> 3
    */
  case class Array_Position(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Remove all elements that equal to element from array.
    * @example array_remove(array, element)
    * @example array_remove(array(1, 2, 3, null, 3), 3) ~> [1,2,null]
    */
  case class Array_Remove(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the array containing element count times.
    * @example array_repeat(element, count)
    */
  case class Array_Repeat(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Sorts the input array in ascending order. The elements of the input array must be orderable.
    * Null elements will be placed at the end of the returned array.
    * @example array_sort(array)
    */
  case class Array_Sort(field: Expression) extends SQLFunction1

  /**
    * Returns an array of the elements in the union of array1 and array2, without duplicates.
    * @example array_union(array1, array2)
    */
  case class Array_Union(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.
    * @example arrays_zip(a1, a2, ...)
    */
  case class Arrays_Zip(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns the numeric value of the first character of str.
    * @example ascii(str)
    */
  case class Ascii(field: Expression) extends SQLFunction1

  /**
    * Returns the inverse sine (a.k.a. arc sine) the arc sin of expr, as if computed by  java.lang.Math.asin.
    * @example asin(expr)
    */
  case class Asin(field: Expression) extends SQLFunction1

  /**
    * Throws an exception if expr is not true.
    * @example assert_true(expr)
    */
  case class Assert_True(field: Expression) extends SQLFunction1

  /**
    * Returns the inverse tangent (a.k.a. arc tangent) of expr, as if computed by java.lang.Math.atan
    * @example atan(expr)
    */
  case class Atan(field: Expression) extends SQLFunction1

  /**
    * Returns the angle in radians between the positive x-axis of a plane and the point given by the coordinates (exprX, exprY), as if computed by java.lang.Math.atan2.
    * @example atan2(exprY, exprX)
    */
  case class Atan2(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the mean calculated from values of a group.
    * @example avg(expr)
    */
  case class Avg(field: Expression) extends SQLFunction1 with Aggregation

  /**
    * Converts the argument from a binary bin to a base 64 string.
    * @example base64(bin)
    */
  case class Base64(field: Expression) extends SQLFunction1

  /**
    * Casts the value expr to the target data type bigint.
    * @example bigint(expr)
    */
  case class BigInt(field: Expression) extends SQLFunction1

  /**
    * Returns the string representation of the long value expr represented in binary.
    * @example bin(expr)
    */
  case class Bin(field: Expression) extends SQLFunction1

  /**
    * Casts the value expr to the target data type binary.
    * @example binary(expr)
    */
  case class Binary(field: Expression) extends SQLFunction1

  /**
    * Returns the bit length of string data or number of bits of binary data.
    * @example bit_length(expr)
    */
  case class Bit_Length(field: Expression) extends SQLFunction1

  /**
    * Casts the value expr to the target data type boolean.
    * @example boolean(expr)
    */
  case class BooleanF(field: Expression) extends SQLFunction1

  /**
    * Returns expr rounded to d decimal places using HALF_EVEN rounding mode.
    * @example bround(expr, d)
    */
  case class Bround(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the size of an array or a map. The function returns -1 if its input is null and
    * spark.sql.legacy.sizeOfNull is set to true. If spark.sql.legacy.sizeOfNull is set to false,
    * the function returns null for null input. By default, the spark.sql.legacy.sizeOfNull parameter
    * is set to true.
    * @example cardinality(expr)
    */
  case class Cardinality(field: Expression) extends SQLFunction1

  /**
    * Casts the value expr to the target data type type.
    * @example cast(expr AS type)
    */
  case class Cast(value: Expression, toType: ColumnType) extends SQLFunction

  /**
    * Returns the cube root of expr.
    * @example cbrt(expr)
    */
  case class Cbrt(field: Expression) extends SQLFunction1

  /**
    * Returns the smallest integer not smaller than expr.
    * @example ceil(expr)
    */
  case class Ceil(field: Expression) extends SQLFunction1

  /**
    * Returns the ASCII character having the binary equivalent to expr.
    * If n is larger than 256 the result is equivalent to chr(n % 256)
    * @example char(expr)
    */
  case class CharF(field: Expression) extends SQLFunction1

  /**
    * Returns the character length of string data or number of bytes of binary data. The length of string data includes the trailing spaces. The length of binary data includes binary zeros.
    * @example char_length(expr)
    */
  case class Char_Length(field: Expression) extends SQLFunction1

  /**
    * Returns the first non-null argument if exists. Otherwise, null.
    * @example coalesce(expr1, expr2, ...)
    */
  case class Coalesce(args: List[Expression]) extends SQLFunctionN

  /**
    * Collects and returns a list of non-unique elements.
    * @example collect_list(expr)
    */
  case class Collect_List(field: Expression) extends SQLFunction1

  /**
    * Collects and returns a set of unique elements.
    * @example collect_set(expr)
    */
  case class Collect_Set(field: Expression) extends SQLFunction1

  /**
    * Returns the concatenation of col1, col2, ..., colN.
    * @example concat(col1, col2, ..., colN)
    */
  case class Concat(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns the concatenation of the strings separated by sep.
    * @example concat_ws(sep, [str | array(str)]+)
    */
  case class Concat_Ws(args: List[Expression]) extends SQLFunctionN

  /**
    * Convert num from from_base to to_base.
    * @example conv(num, from_base, to_base)
    */
  case class Conv(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns Pearson coefficient of correlation between a set of number pairs.
    * @example corr(expr1, expr2)
    */
  case class Corr(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the cosine of expr, as if computed by java.lang.Math.cos.
    * @example cos(expr)
    */
  case class Cos(field: Expression) extends SQLFunction1

  /**
    * Returns the hyperbolic cosine of expr, as if computed by java.lang.Math.cosh.
    * @example cosh(expr)
    */
  case class CosH(field: Expression) extends SQLFunction1

  /**
    * Returns the cotangent of expr, as if computed by 1/java.lang.Math.cot.
    * @example cot(expr)
    */
  case class Cot(field: Expression) extends SQLFunction1

  /**
    * Returns the total number of retrieved rows, including rows containing null.
    * @example count(*)
    *
    * Returns the number of rows for which the supplied expression(s) are all non-null.
    * @example count(expr[, expr...])
    *
    * Returns the number of rows for which the supplied expression(s) are unique and non-null.
    * @example count(DISTINCT expr[, expr...])
    */
  case class Count(field: Expression) extends SQLFunction1 with Aggregation

  /**
    * Returns a count-min sketch of a column with the given esp, confidence and seed.
    * The result is an array of bytes, which can be deserialized to a CountMinSketch before usage.
    * Count-min sketch is a probabilistic data structure used for cardinality estimation using sub-linear space.
    * @example count_min_sketch(col, eps, confidence, seed)
    */
  case class Count_Min_Sketch(args: List[Expression]) extends SQLFunctionN with Aggregation

  /**
    * Returns the population covariance of a set of number pairs.
    * @example covar_pop(expr1, expr2)
    */
  case class Covar_Pop(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the sample covariance of a set of number pairs.
    * @example covar_samp(expr1, expr2)
    */
  case class Covar_Samp(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns a cyclic redundancy check value of the expr as a bigint.
    * @example crc32(expr)
    */
  case class CRC32(field: Expression) extends SQLFunction1

  /**
    * @see [[https://spark.apache.org/docs/2.3.1/api/sql/index.html#cube]]
    */
  case class Cube(args: List[Expression]) extends SQLFunctionN

  /**
    * Computes the position of a value relative to all values in the partition.
    * @example cume_dist()
    */
  case object Cume_Dist extends SQLFunction

  /**
    * Returns the current database.
    * @example current_database()
    */
  case object Current_Database extends SQLFunction

  /**
    * Returns the current date at the start of query evaluation.
    * @example current_date()
    */
  case object Current_Date extends SQLFunction

  /**
    * Returns the current timestamp at the start of query evaluation.
    * @example current_timestamp()
    */
  case object Current_Timestamp extends SQLFunction

  /**
    * Casts the value expr to the target data type date.
    * @example date(expr)
    */
  case class Date(field: Expression) extends SQLFunction1

  /**
    * Returns the date that is num_days after start_date.
    * @example date_add(start_date, num_days)
    */
  case class Date_Add(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Converts timestamp to a value of string in the format specified by the date format fmt.
    * @example date_format(timestamp, fmt)
    */
  case class Date_Format(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the date that is num_days before start_date.
    * @example date_sub(start_date, num_days)
    */
  case class Date_Sub(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns timestamp ts truncated to the unit specified by the format model fmt.
    * fmt should be one of ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD", "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"]
    * @example date_trunc(fmt, ts)
    */
  case class Date_Trunc(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the number of days from startDate to endDate.
    * @example datediff(endDate, startDate)
    */
  case class DateDiff(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the day of month of the date/timestamp.
    * @example day(date)
    */
  case class Day(field: Expression) extends SQLFunction1

  /**
    * Returns the day of month of the date/timestamp.
    * @example dayofmonth(date)
    */
  case class DayOfMonth(field: Expression) extends SQLFunction1

  /**
    * Returns the day of the week for date/timestamp (1 = Sunday, 2 = Monday, ..., 7 = Saturday).
    * @example dayofweek(date)
    */
  case class DayOfWeek(field: Expression) extends SQLFunction1

  /**
    * Returns the day of year of the date/timestamp.
    * @example dayofyear(date)
    */
  case class DayOfYear(field: Expression) extends SQLFunction1

  /**
    * Casts the value expr to the target data type decimal.
    * @example decimal(expr)
    */
  case class Decimal(field: Expression) extends SQLFunction1

  /**
    * Decodes the first argument using the second argument character set.
    * @example decode(bin, charset)
    */
  case class Decode(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Converts radians to degrees.
    * @example degrees(expr)
    */
  case class Degrees(field: Expression) extends SQLFunction1

  /**
    * Computes the rank of a value in a group of values. The result is one plus the previously assigned rank value.
    * Unlike the function rank, dense_rank will not produce gaps in the ranking sequence.
    * @example dense_rank()
    */
  case object Dense_Rank extends SQLFunction

  /**
    *
    * @example distinct(expr)
    */
  case class Distinct(field: Expression) extends SQLFunction1 with Aggregation

  /**
    * Casts the value expr to the target data type double.
    * @example double(expr)
    */
  case class DoubleF(field: Expression) extends SQLFunction1

  /**
    * Returns Euler's number, e.
    * @example e()
    */
  case object E extends SQLFunction

  /**
    * Returns the n-th input, e.g., returns input2 when n is 2.
    * @example elt(n, input1, input2, ...)
    */
  case class Elt(args: List[Expression]) extends SQLFunctionN

  /**
    * Encodes the first argument using the second argument character set.
    * @example encode(str, charset)
    */
  case class Encode(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns e to the power of expr.
    * @example exp(expr)
    */
  case class Exp(field: Expression) extends SQLFunction1

  /**
    * Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.
    * @example explode(expr)
    */
  case class Explode(field: Expression) extends SQLFunction1

  /**
    * Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.
    * @example explode_outer(expr)
    */
  case class Explode_Outer(field: Expression) extends SQLFunction1

  /**
    * Returns exp(expr) - 1.
    * @example expm1(expr)
    */
  case class Expm1(field: Expression) extends SQLFunction1

  /**
    * Returns the factorial of expr. expr is [0..20]. Otherwise, null.
    * @example factorial(expr)
    */
  case class Factorial(field: Expression) extends SQLFunction1

  /**
    * Returns the index (1-based) of the given string (str) in the comma-delimited list (str_array).
    * Returns 0, if the string was not found or if the given string (str) contains a comma.
    * @example find_in_set(str, str_array)
    */
  case class Find_In_Set(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the first value of expr for a group of rows. If isIgnoreNull is true, returns only non-null values.
    * @example first(expr[, isIgnoreNull])
    * @example first_value(expr[, isIgnoreNull])
    */
  case class First_Value(args: List[Expression]) extends SQLFunctionN

  /**
    * Transforms an array of arrays into a single array.
    * @example flatten(arrayOfArrays)
    */
  case class Flatten(field: Expression) extends SQLFunction1

  /**
    * Casts the value expr to the target data type float.
    * @example float(expr)
    */
  case class FloatF(field: Expression) extends SQLFunction1

  /**
    * Returns the largest integer not greater than expr.
    * @example floor(expr)
    */
  case class Floor(field: Expression) extends SQLFunction1

  /**
    * Formats the number expr1 like '#,###,###.##', rounded to expr2 decimal places.
    * If expr2 is 0, the result has no decimal point or fractional part. expr2 also accept a user specified format.
    * This is supposed to function like MySQL's FORMAT.
    * @example format_number(expr1, expr2)
    */
  case class Format_Number(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns a formatted string from printf-style format strings.
    * @example format_string(strfmt, obj, ...)
    */
  case class Format_String(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns a struct value with the given jsonStr and schema.
    * @example from_json(jsonStr, schema[, options])
    */
  case class From_JSON(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns unix_time in the specified format.
    * @example from_unixtime(unix_time, format)
    */
  case class From_UnixTime(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Given a timestamp like '2017-07-14 02:40:00.0', interprets it as a time in UTC,
    * and renders that time as a timestamp in the given time zone.
    * For example, 'GMT+1' would yield '2017-07-14 03:40:00.0'.
    * @example from_utc_timestamp(timestamp, timezone)
    */
  case class From_UTC_Timestamp(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Extracts a json object from path.
    * @example get_json_object(json_txt, path)
    */
  case class Get_JSON_Object(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * Returns the greatest value of all parameters, skipping null values.
    * @example greatest(expr, ...)
    */
  case class Greatest(args: List[Expression]) extends SQLFunctionN

  /**
    * Returns a hash value of the arguments.
    * @example hash(expr1, expr2, ...)
    */
  case class Hash(args: List[Expression]) extends SQLFunctionN

  /**
    * Converts expr to hexadecimal.
    * @example hex(expr)
    */
  case class Hex(field: Expression) extends SQLFunction1

  /**
    * Returns the hour component of the string/timestamp.
    * @example hour(timestamp)
    */
  case class Hour(field: Expression) extends SQLFunction1

  /**
    * Returns sqrt(expr12 + expr22).
    * @example hypot(expr1, expr2)
    */
  case class Hypot(field1: Expression, field2: Expression) extends SQLFunction2

  /**
    * If expr1 evaluates to true, then returns expr2; otherwise returns expr3.
    * @example if(expr1, expr2, expr3)
    */
  case class If(condition: Condition, trueValue: Expression, falseValue: Expression) extends SQLFunction

  /**
    * Returns expr2 if expr1 is null, or expr1 otherwise.
    * @example ifnull(expr1, expr2)
    */
  case class IfNull(condition: Condition, field2: Expression) extends SQLFunction

  case class Length(field: Expression) extends SQLFunction1

  case class Lower(field: Expression) extends SQLFunction1

  case class LPad(field1: Expression, field2: Expression, field3: Expression) extends SQLFunction3

  case class LTrim(field: Expression) extends SQLFunction1

  case class Max(field: Expression) extends SQLFunction1 with Aggregation

  case class Mean(field: Expression) extends SQLFunction1 with Aggregation

  case class Min(field: Expression) extends SQLFunction1 with Aggregation

  case class RPad(field1: Expression, field2: Expression, field3: Expression) extends SQLFunction3

  case class RTrim(field: Expression) extends SQLFunction1

  case class Split(field1: Expression, field2: Expression) extends SQLFunction2

  case class Substring(field1: Expression, field2: Expression, field3: Expression) extends SQLFunction3

  case class Sum(field: Expression) extends SQLFunction1 with Aggregation

  case class StdDev(field: Expression) extends SQLFunction1 with Aggregation

  case class To_Date(field: Expression) extends SQLFunction1

  case class Trim(field: Expression) extends SQLFunction1

  /**
    * Converts hexadecimal expr to binary.
    * @example unhex(expr)
    */
  case class UnHex(field: Expression) extends SQLFunction1

  case class Upper(field: Expression) extends SQLFunction1

  case class Variance(field: Expression) extends SQLFunction1 with Aggregation

  case class WeekOfYear(field: Expression) extends SQLFunction1

  case class Year(field: Expression) extends SQLFunction1

}