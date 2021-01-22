package com.hipages.jsontransformer.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.zalando.spark.jsonschema.SchemaConverter
import org.apache.log4j.{Level, Logger}

object DataHandlingUtil {

  val log: Logger = Logger.getLogger(getClass)
  log.setLevel(Level.INFO)

  /**
   * if fourth argument is "validate", the below method will be called which
   * will validate the input data and writes the invalid data, if any, to disk
   *
   * @param validator  : Fourth argument to spark-submit ("validate"/"no")
   * @param inputFile  : Input path
   * @param schema     : schema.json file
   * @param outputFile : output path
   */
  def validateInput(validator: String, inputFile: String, schema: StructType, outputFile: String, spark: SparkSession): Unit = {

    if (validator == "validate") {
      val invalidDf = AuditCountsUtil.findInvalid(inputFile, schema, spark)
      writeToFileAsCsv(invalidDf, outputFile + "/invalid/malformed")

      val corruptDf = AuditCountsUtil.findCorrupt(inputFile, schema, spark)
      writeToText(corruptDf.toDF(), outputFile + "/invalid/corrupt")
      log.info("Json input validated for corrupt/malformed/invalid items")
    }
    else log.info("*** Count Validation Skipped ***")
  }

  /**
   * Get/Validate schema from Zalando library
   * ref:- https://github.com/zalando-incubator/spark-json-schema
   *
   * @param schemaFile : String value of schem json
   * @return: schema as StructType
   */
  def getSchema(schemaFile: String): StructType = {

    val jsonValidation = readSchema(schemaFile)
    SchemaConverter.convertContent(jsonValidation)
  }

  /**
   * Read schema string from input schema file
   *
   * @param schemaPath : Schema.json content as String
   * @return: schema as String
   */
  def readSchema(schemaPath: String): String = {

    val json_schema = scala.io.Source.fromFile(schemaPath)
    val json_schema_string = json_schema.mkString
    json_schema.close()
    json_schema_string
  }

  /**
   * Receives Dataframe & Outputh path as arguments and writes the dataframe to this path
   * format: CSV
   *
   * @param df         : The dataframe to write to file
   * @param outputPath : Path to write the dataframe to
   */
  def writeToFileAsCsv(df: DataFrame, outputPath: String) {

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath)
  }

  /**
   * Receives Dataframe & Outputh path as arguments and writes the dataframe to this path
   * tformat: text
   *
   * @param df         : The dataframe to write to file
   * @param outputPath : Path to write dataframe file to
   */
  def writeToText(df: DataFrame, outputPath: String) {

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .text(outputPath)
  }
}
