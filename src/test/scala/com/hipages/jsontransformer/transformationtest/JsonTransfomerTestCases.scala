package com.hipages.jsontransformer.transformationtest

import com.hipages.jsontransformer.constants.AppConstants
import com.hipages.jsontransformer.utils.{AuditCountsUtil, DataHandlingUtil}
import com.hipages.jsontransformer.transformations.{TransformationActivityCounts, TransformationUserActivity}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

case class ActivitySus(user_id: String, time_stamp: String, url_level1: String, url_level2: String, url_level3: String, activity: String)

case class ActivityErr(user_id: Long, time_stamp: String, url_level1: String, url_level2: String, url_level3: String, activity: String)

case class jsonLine(event_id: String, user: String, action: String, url: String, timestamp: String)

case class ActivityUserCount(time_bucket: String, url_level_1: String, url_level_2: String, activity: String, activity_count: Int, user_count: Int)

class ActivityTransformationTestSuccessScenario extends FunSuite with SharedSparkContext {

  /**
   * Test Case - 1
   * Events to Table Format success case
   */
  test("TransformationUserActivityTest") {

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    import spark.implicits._

    val inputFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_event_data_1.json"
    val schemaFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_data_schema.json"

    val schema = DataHandlingUtil.getSchema(schemaFile)
    val inputDf = spark
      .read
      .schema(schema)
      .option("mode", AppConstants.correctiveActionNormal)
      .json(inputFile)

    val transformedDf = TransformationUserActivity.transformDf(inputDf).collect()

    val expectedDf = Seq(ActivitySus("56456", "02/02/2017 20:22:00", "hipages.com", "articles", null, "page_view")).toDF().collect()

    assert(transformedDf.sameElements(expectedDf))
  }

  /**
   * Test Case - 2
   * Events to Table Format failure case
   */
  test("TransformationUserActivityTestErrorCase") {

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
    import spark.implicits._

    val inputFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_event_data_malformed_corrupt.json"
    val schemaFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_data_schema_updated.json"

    val schema = DataHandlingUtil.getSchema(schemaFile)
    val inputDf = spark
      .read
      .schema(schema)
      .option("mode", AppConstants.correctiveActionNormal)
      .json(inputFile)

    val transformedDf = TransformationUserActivity.transformDf(inputDf).collect()

    val expectedDf = Seq(ActivityErr(56456, "02/02/2017 20:26:00", "hipages.com", "get_quotes_simple?search_str=sfdg", null, "page_view"),
      ActivityErr(56456, "01/03/2017 20:21:00", "hipages.com", "advertise", null, "button_click")).toDF().collect()

    assert(transformedDf.sameElements(expectedDf))
  }

  /**
   * Test Case - 3
   * Catching an Invalid json test case
   */
  test("AuditCountsUtilInvalidTest") {

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    import spark.implicits._

    val inputFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_event_data_err.json"
    val schemaFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_data_schema_updated.json"

    val schema = DataHandlingUtil.getSchema(schemaFile)
    val invalidDf = AuditCountsUtil.findInvalid(inputFile, schema, spark).collect()

    val expectedDf = Seq(jsonLine(null, null, null, null, null)
      , jsonLine("324872349721534", null, "page_view", "https://www.hipages.com.au/get_quotes_simple?search_str=sfdg", "02/02/2017 20:26:00")).toDF().collect()

    assert(invalidDf.sameElements(expectedDf))
  }

  /**
   * Test Case - 4
   * Catching a corrupt json test case
   */
  test("AuditCountsUtilCorruptTest") {

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    import spark.implicits._

    val inputFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_event_data_err.json"
    val schemaFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_data_schema_updated.json"

    val schema = DataHandlingUtil.getSchema(schemaFile)
    val invalidDf = AuditCountsUtil.findCorrupt(inputFile, schema, spark).collect()

    val expectedDf = Seq("{ \"event_id : \"349824093287032\", \"user\" : { \"session_id\" : \"564562\", \"id\" : 56456 , \"ip\" : \"111.222.333.5\" }, \"action\" : \"page_view\", \"url\" : \"https://www.hipages.com.au/connect/sfelectrics/service/190625\", \"timestamp\" : \"02/02/2017 20:23:00\"}")
      .toDF().collect()

    assert(invalidDf.sameElements(expectedDf))
  }

  /**
   * Test Case - 5
   * Events to Activity counts test case
   */
  test("TransformActivityCountsTest") {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    import spark.implicits._

    val inputFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_event_data_1.json"
    val schemaFile = "/home/thesnibibin/Desktop/hipages/ETLAppJsonTransformer/src/test/resources/source_data_schema.json"

    val schema = DataHandlingUtil.getSchema(schemaFile)
    val inputDf = spark
      .read
      .schema(schema)
      .option("mode", AppConstants.correctiveActionNormal)
      .json(inputFile)

    val transformedDf = TransformationActivityCounts.transformDf(TransformationUserActivity.transformDf(inputDf)).collect()

    val expectedDf = Seq(ActivityUserCount("2017020200", "hipages.com", "articles", "page_view", 1, 1)).toDF().collect()

    assert(transformedDf.sameElements(expectedDf))
  }
}
