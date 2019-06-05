package com.chrisluttazi.spark.etl.commons

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.util.Calendar
import java.text.SimpleDateFormat

// First run for Slowly changing Dimension
// The code enables to check and load history table and handle slowly changing data by using start date and effective date
object ScdFirstRun {
  def main(args: Array[String]): Unit = {

    println("SCD First run started")
    
    //definining a variable to pass current date value in Inputfile variable 
    val proc_dt = args(0)
    
    //Initiating 2 date format variables in diffrent format to be used to later while assigning values yo eff_dt and exp_dt 
    val inputFormat = new SimpleDateFormat("yyyyMMdd")
    val reqFormat = new SimpleDateFormat("yyyy-MM-dd")

    // Convert the effective and expiry date in yyyy-MM-dd format    

    val eff_dt = reqFormat.format(inputFormat.parse(proc_dt))
    val exp_dt = reqFormat.format(reqFormat.parse("2099-12-31"))    
    
    // histpath sets path for storing history data of the table
    val histPath = new Path("/user/singhanurag/scala/hist/")
    
    //histTabPath is being assigned the path value 
    val histTabPath = "/user/singhanurag/scala/hist/"

    //input_file being assign a value of landing directory with file name containing date using string interpolation
    val input_file = s"/user/singhanurag/data/user_$proc_dt.csv"
    
    // spark conf defined for creating spark context. uses parameters like application name and master node.
    val conf = new SparkConf().setAppName("scd-2-first-run").setMaster("yarn-client")
    
    //sc or spark context being created using conf.
    val sc = new SparkContext(conf)
    
    //sqlContext defined for defining number of partitions to be made in case of data shuffeling for join
    val sqlContext = new SQLContext(sc)
    
    //variable fs defined to perform filesystem level operations like checking condition, creating directories.
    val fs = FileSystem.get(sc.hadoopConfiguration)
    
    //configuration for defining partitions
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    
    //configuration for enabling ORC read and write
    sqlContext.setConf("spark.sql.orc.enabled","true")

    import sqlContext.implicits._
    
    // Check if the HIST table directory present for first run, else create directory    

    if (!fs.exists(histPath)) {
      println("History table directory does not exist, creating directory")
      fs.mkdirs(histPath)
      
    } else {
            println("History table directory exist")
            }

    // Read User data from the file and set effective and expiry date
    val user_data = sqlContext.read.format("com.databricks.spark.csv").
                                    option("header", "true").
                                    option("inferSchema", "true").
                                    option("quote", "\"").
                                    option("ignoreLeadingWhiteSpace", true).
                                    load(input_file)
    
    // assigning user_data_hist with eff_dt and exp_dt columns in designated format
    val user_data_hist = user_data.withColumn("eff_dt",to_date(lit(eff_dt),"yyyy-MM-dd")).withColumn("exp_dt",to_date(lit(exp_dt),"yyyy-MM-dd"))

    // Save updated user data to HIST table 
    user_data_hist.coalesce(2).write.mode(SaveMode.Overwrite).format("orc").save(histTabPath)

    println("First run completed successfully")
  }
