package com.hipages.jsontransformer.utils

import com.hipages.jsontransformer.constants.AppConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType

/**
 * If validate option is selected in command line as "validate",
 * this method validates the input json and writes malformed/invalid
 * records to disk. However, these records are dropped from further processing
 * in main dataframe
 */
object AuditCountsUtil {

  /**
   * Converts struct to string type to be able to write as file
   * ref:- https://stackoverflow.com/questions/40426106/spark-2-0-x-dump-a-csv-file-from-a-dataframe-containing-one-array-of-type-string
   */
  val stringify: UserDefinedFunction = udf((vs: Seq[String]) => vs match {
    case null => null
    case _ => s"""[${vs.mkString(",")}]"""
  })

  /**
   * find invalid records (datatyping, range error items etc)
   *
   * @param input  : Input file path
   * @param schema : Schema file path
   * @param spark  : sessio
   * @return: Invalid Dataframe
   */
  def findInvalid(input: String, schema: StructType, spark: SparkSession): DataFrame = {

    val errDf = spark
      .read
      .schema(schema)
      .option("mode", AppConstants.correctiveActionAbnormal)
      .json(input)

    val nonErrDf = spark
      .read
      .schema(schema)
      .option("mode", AppConstants.correctiveActionNormal)
      .json(input)

    val invalidDf = nonErrDf.unionAll(errDf).except(nonErrDf.intersect(errDf))

    invalidDf.withColumn("user", stringify(col("user")))
      .select(
        col("event_id"),
        col("user"),
        col("action"),
        col("url"),
        col("timestamp"))
  }

  /**
   * Find corrupt records. (Malformed, invalid json lines etc)
   *
   * @param input  : Input file path
   * @param schema : Schema file path
   * @param spark  : session
   * @return: Corrupt Dataframe
   */
  def findCorrupt(input: String, schema: StructType, spark: SparkSession): DataFrame = {

    val fullDf = spark
      .read
      .json(input)

    fullDf
      .select(
        col("_corrupt_record").alias("corrupt_record"),
        col("user")).filter("user is null")
      .select(col("corrupt_record"))
  }
}
