package com.me.jsontransformer.entry

import com.me.jsontransformer.constants.AppConstants
import com.me.jsontransformer.transformations.{TransformationActivityCounts, TransformationUserActivity}
import com.me.jsontransformer.utils.DataHandlingUtil
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
 * Main Application code.
 * Calls methods to transform input json to structured form
 */

object ETLMainApp {

  val log: Logger = Logger.getLogger(getClass)
  log.setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    /**
     * Check number of arguments.
     * To be enhanced:- to use scala parser using pattern matching
     */
    if (args.length != 4) {
      log.error(s"You have supplied ${args.length} arguments")
      log.error("Usage is wrong. Please supply 4 arguments as below.")
      log.error("<input_file_path> <output_file_path> <schema_file_path> <validate>(values: validate or no)")
    }
    else {
      val argumentsToApp = (args(0), args(1), args(2), args(3))

      val inputFile = AppArguments(argumentsToApp).inputFile
      val outputFile = AppArguments(argumentsToApp).targetFolder
      val schemaFile = AppArguments(argumentsToApp).schemaFile
      val validator = AppArguments(argumentsToApp).validator

      implicit val spark: SparkSession = SparkSession
        .builder()
        .appName("Hipages Json Reader")
        .config("spark.master", "local")
        .getOrCreate()

      val schema = DataHandlingUtil.getSchema(schemaFile)
      log.info("Schema received from file.")

      if (validator == "validate") {
        log.info("Validating Json for errors. Please check invalid dir in targte folder if issues exist")
        DataHandlingUtil.validateInput(validator, inputFile, schema, outputFile, spark)
      }
      else {
        log.info("***Validation Skipped***")
        log.info("However, program recommends validating the json")
      }

      val inputDf = spark
        .read
        .schema(schema)
        .option("mode", AppConstants.correctiveActionNormal)
        .json(inputFile)

      val activityTransformedDf = TransformationUserActivity.transformDf(inputDf)
      activityTransformedDf.cache()

      DataHandlingUtil.writeToFileAsCsv(activityTransformedDf, outputFile + "/activity")
      log.info("Activity report written to csv in path " + outputFile + "/activity")
      DataHandlingUtil.writeToFileAsCsv(TransformationActivityCounts.transformDf(activityTransformedDf), outputFile + "/aggregate")
      log.info("Activity-User counts report written to csv in path " + outputFile + "/aggregate")
    }
  }
}
