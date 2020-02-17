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
    
    // definining a variable proc_dt to pass current date value in Inputfile variable
    // a runtime variable which will use date that will be passed during execution of spark job.
    // spark-submit --class ScdFirstRun --master yarn --conf spark.ui.port=<port Number default is 4040> --num-executors 4 
    // --executor-memory 1GB <jar path> target/scala-2.11/slowly-changing-dimention-2_<spark_version>.jar "20180731" <proc_dt>
    val proc_dt = args(0)
    
    // Initiating 2 date format variables in diffrent format to be used to later while assigning values to eff_dt and exp_dt
    // inputFormat is assigned a date format as yyyyMMdd as this will be the format in the incoming source file.

    val inputFormat = new SimpleDateFormat("yyyyMMdd")
    
    // reqFormat or required format is assigned a different date format as yyyy-MM-dd as we want to maintain similar format for handling     // dates when there is a change in record entry
    // This conversion enables to handle date effectively in hive 
    val reqFormat = new SimpleDateFormat("yyyy-MM-dd")

    // Convert the effective and expiry date in yyyy-MM-dd format.
    // variable eff_dt and exp_dt utilises previously initialised inputFormat and reqFormat to be parsed with date values.
    
    val eff_dt = reqFormat.format(inputFormat.parse(proc_dt))
    val exp_dt = reqFormat.format(reqFormat.parse("2099-12-31"))    
    
    // histpath sets path for storing history data of the table
    // This can be checked in HDFS environment where Hive is being hosted
    val histPath = new Path("/user/singhanurag/scala/hist/")
    
    // histTabPath is being assigned the path value.
    // 
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
    // This property will distribute data set when using spark sql queries
    // number 10 signifies number of partitions and is taken as an example. real number can be calculates by total size/total spark            mappers
    // we can see under stages UI how this is being mapped
    sqlContext.setConf("spark.sql.shuffle.partitions", "10")
    
    // configuration for enabling ORC read and write
    // This property enables using ORC format to write data. ORC is optimized row columnar. It is extremely compatible with Hive.
    // it is also helpful during faster data retreival and also provides high compression.
    // We can also use sqlContext.setConf("spark.sql.orc.enableVectorizedReader","true").
    // This property can be set on the tables or during runtime.
    sqlContext.setConf("spark.sql.orc.enabled","true")

    // import sqlContext.implicits._
    // It can only be imported once we have created an instance of org.apache.spark.sql.SQLContext which is created above
    import sqlContext.implicits._
    
    // Check if the HIST table directory present for first run, else create directory    

    if (!fs.exists(histPath)) {
      println("History table directory does not exist, creating directory")
      fs.mkdirs(histPath)
      
    } else {
            println("History table directory exist")
            }

    // Read User data from the file and set effective and expiry date
    // It has some options that can be set to either true or false depending upon the source data file
    // when header is set to true it will read the header of the file as well
    // if inferSchema is set to true it will fetch schema from source along with exact data types.
    // option quote will omit quotes in the data file while reading it
    // option ingonerLeadngWhiteSpace will ignore spaces within the value of a particular column
    val user_data = sqlContext.read.format("com.databricks.spark.csv").
                                    option("header", "true").
                                    option("inferSchema", "true").
                                    option("quote", "\"").
                                    option("ignoreLeadingWhiteSpace", true).
                                    load(input_file)
    
    // assigning user_data_hist with eff_dt and exp_dt columns in designated format
    // this will include eff_dt and exp_dt in yyyy-MM-dd format in variable user_data_hist which will be used to write data in               // histTabPath. It will check condition on user_data read above and include eff_dt and exp_dt values assigned above.
   
    val user_data_hist = user_data.withColumn("eff_dt",to_date(lit(eff_dt),"yyyy-MM-dd")).withColumn("exp_dt",to_date(lit(exp_dt),"yyyy-MM-dd"))

    // Save updated user data to HIST table 
    user_data_hist.coalesce(2).write.mode(SaveMode.Overwrite).format("orc").save(histTabPath)

    println("First run completed successfully")
  }
