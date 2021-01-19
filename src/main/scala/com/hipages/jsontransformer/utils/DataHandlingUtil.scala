package com.hipages.jsontransformer.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import org.zalando.spark.jsonschema.SchemaConverter
import org.slf4j.{Logger, LoggerFactory}

object DataHandlingUtil {

  val log: Logger = LoggerFactory.getLogger(getClass)

  /**
   * if fourth argument is "validate", the below method will be called which will validate the input data
   * @param validator: Fourth (optional) argument to spark-submit
   * @param inputFile: Input path
   * @param schema: schema.json file
   * @param outputFile: output path
   */
  def validateInput(validator:String, inputFile: String, schema: StructType, outputFile: String, spark: SparkSession): Unit = {
    if (validator == "validate") {
      val invalidDf = AuditCountsUtil.findInvalid(inputFile, schema, spark)
      writeToFileAsCsv(invalidDf, outputFile + "/invalid/malformed")

      val corruptDf = AuditCountsUtil.findCorrupt(inputFile, schema, spark)
      writeToText(corruptDf.toDF(), outputFile + "/invalid/corrupt")
      log.info("Json input validated for corrupt/malformed/invalid items")
    }
    else println("*** Count Validation Skipped ***")
  }

  /**
   * Get/Validate schema from Zalando library
   * https://github.com/zalando-incubator/spark-json-schema
   *
   * @param schemaFile: String value of schem json
   * @return schema
   */

  def getSchema(schemaFile: String): StructType = {

    val jsonValidation = readSchema(schemaFile)
    SchemaConverter.convertContent(jsonValidation)
  }

  /**
   * Read schema string from input schema file
   *
   * @param schemaPath: Schema.json content as String
   * @return
   */
  def readSchema(schemaPath: String): String = {

    val json_schema = scala.io.Source.fromFile(schemaPath)
    val json_schema_string = json_schema.mkString
    json_schema.close()
    json_schema_string
  }

  /**
   * Receives Dataframe & outputh path as arguments and writes the DF as a csv to the output path
   * CSV format
   *
   * @param df         : The dataframe to write to file
   * @param outputPath : Path to write the file to
   */
  def writeToFileAsCsv(df: DataFrame, outputPath: String) {

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(outputPath)
  }

  /**
   * Receives Dataframe & outputh path as arguments and writes the DF as a csv to the output path
   * text format
   *
   * @param df         : The dataframe to write to file
   * @param outputPath : Path to write the file to
   */
  def writeToText(df: DataFrame, outputPath: String) {

    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .text(outputPath)
  }
}
