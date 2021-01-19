package com.hipages.jsontransformer.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Accepts the {user_id,time_stamp,url_level1,url_level2,
 * url_level3,activity} dataframe and
 * aggregates over the dataframe to find the {activity count and
 * user count} on a hourly level granularity
 * https://stackoverflow.com/questions/46353360/use-length-function-in-substring-in-spark
 */
object TransformationActivityCounts {

  def transformDf(df: DataFrame): DataFrame = {
    df
      .withColumn("time_bucket", date_format(to_date(col("time_stamp"), "dd/MM/yyyy HH:mm:ss"), "yyyyMMddHH"))
      .select(
        col("user_id"),
        col("time_bucket"),
        col("url_level1"),
        col("url_level2"),
        col("url_level3"),
        col("activity"))
      .groupBy("time_bucket", "url_level1", "url_level2", "activity")
      .agg(count(
        col("activity")).alias("activity_count"),
        countDistinct(
          col("user_id")).alias("user_count"))
  }
}
