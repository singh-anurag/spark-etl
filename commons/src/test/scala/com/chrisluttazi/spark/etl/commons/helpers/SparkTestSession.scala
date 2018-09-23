package com.chrisluttazi.spark.etl.commons.helpers

import org.apache.spark.sql.SparkSession

class SparkTestSession {
  def get: SparkSession =
    SparkSession.builder
      .master("local")
      .appName("SparkTestSession")
      .enableHiveSupport()
      .getOrCreate()
}

object SparkTestSession extends SparkTestSession
