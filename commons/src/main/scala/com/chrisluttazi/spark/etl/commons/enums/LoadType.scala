package com.chrisluttazi.spark.etl.commons.enums

object LoadType extends Enumeration {
  type LoadType = Value
  val ORC = Value("orc")
  val PARQUET = Value("parquet")
  val JSON = Value("json")
  val CSV = Value("csv")
}
