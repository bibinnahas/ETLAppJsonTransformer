package com.me.jsontransformer.transformations

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, split}

/**
 * Accepts input dataframe as an argument and transforms it to table format
 * {user_id 	time_stamp 	url_level1 	url_level2 	url_level3 	activity}
 *
 */
object TransformationUserActivity {

  /**
   *
   * @param df : receives input dataframe
   * @return: returns transformed dataframe
   */
  def transformDf(df: DataFrame): DataFrame = {

    df.withColumn("full_url", split(col("url"), "\\/"))
      .select(
        col("user.id").alias("user_id"),
        col("timestamp").alias("time_stamp"),
        col("full_url").getItem(2).alias("url_level1_full"),
        col("full_url").getItem(3).alias("url_level2"),
        col("full_url").getItem(4).alias("url_level3"),
        col("action").alias("activity"))
//    ref:- https://stackoverflow.com/questions/46353360/use-length-function-in-substring-in-spark
      .withColumn("url_level1", expr("substring(url_level1_full, 5, length(url_level1_full) - 7)"))
      .select(
        col("user_id"),
        col("time_stamp"),
        col("url_level1"),
        col("url_level2"),
        col("url_level3"),
        col("activity"))
  }
}
